import * as Firebird from 'node-firebird';
import {ZabbixSender} from "./ZabbixSender";
const Options = require('./flagParams').options();
const convertDate = require('./convertDate');
const bufferJson = require('buffer-json');
const exitHook = require('exit-hook');
import * as os from 'os';


const POOL_MAX = 100;
const POOL_HIGH_ALERT = Math.floor(POOL_MAX * 0.4);

const pool = Firebird.pool(POOL_MAX, {
  ...Options,
}) as FirebirdConnectionPool;

export const sqlQuery = (param) => {
  return (req, res) => {
    let result = [];
    let properties;

    if (param === 'health') {
      properties = {
        isTransaction: false,
        statements: undefined,
        sql: 'SELECT NAVN FROM SYSTEM',
      };
    } else {
      properties = req[param];

      if (properties.sharedKey !== process.env['FIREBIRD_SHARED_KEY']) {
        res.status(403);
        res.send('Invalid shared credentials');
      }
    }

    const isTransaction = properties.isTransaction;
    const statements = properties.statements;

    if (isTransaction) {
      if (!statements || !statements.length) {
        return res.send(['No valid SQL statements found! Please enter valid SQL statements.']);
      }
    } else {
      if (!properties.sql) {
        return res.send(['No valid SQL query found! Please enter a valid SQL query.']);
      }
    }

    pool.get(function(err, db) {
      if (err) {
        console.error(err);
        if (db) {
          db.detach()
        }
        res.status(400); // BAD REQUEST
        return res.send(`\n${err.message}\n`);
      }

      if (pool.dbinuse > POOL_HIGH_ALERT) {
        console.error(`ALERT: Connection pool using ${pool.dbinuse} of ${pool.max} connections.`)
      }

      if (isTransaction) {
        db.transaction(Firebird.ISOLATION_REPEATABLE_READ, async (err, transaction) => {
          if (err) {
            db.detach();
            res.status(400); // BAD REQUEST
            return res.send(err.message);
          }

          for(const statement of statements) {
            const { params, sql } = statement;
            try {
              await executeTransactionQuery(transaction, { params: params, sql });
            } catch(err) {
              db.detach();

              res.status(400);
              res.send(err.message);
              return;
            }
          }

          transaction.commit((err) => {
            if (err) {
              transaction.rollback((err) => {
                db.detach();
                res.status(400);
                res.send(err.message);
              });
            } else {
              db.detach();

              res.status(200);
              res.send('OK');
            }
          });
        })
      } else {
        const params = properties.params;
        const sql = properties.sql;
        db.query(sql, params, (err, data) => {
          if (err) {
            db.detach();
            res.status(400); // BAD REQUEST
            return res.send(`\n${err.message}\n`);
          }

          if (param === 'health') {
            db.detach();
            return res.send(JSON.stringify({ healthy: true }));
          } else {
            convertRows(data)
              .finally(() => {
                db.detach();
              })
              .then((result) => {
              let jsonString = bufferJson.stringify(result);

              if (jsonString === undefined) {
                jsonString = '{}';
              }

              res.send(jsonString);
            });
          }
        });
      }
    });
  };
};

function executeTransactionQuery(transaction, statement) {
  const { sql, params } = statement;
  return new Promise((resolve, reject) => {
    transaction.query(sql, params, (err, result) => {
      if (err) {
        transaction.rollback();
        reject(err);
      } else {
        resolve(result);
      }
    })
  });
}

async function convertRows(data) {
  let result: any;
  if (data) {
    if (Array.isArray(data)) {
      result = [];
      // CONVERT RAW QUERY RESULT AND RETURN JSON
      for (const row of data) {
        const newRow = await convertRow(row);
        result.push(newRow);
      }
    } else {
      result = await convertRow(data) as any[];
    }
  }

  return result;
}

async function convertRow(row) {
  let newRow = {};
  for (const el in row) {
    newRow[el] = row[el];
    if (row[el] instanceof Date) {
      newRow[el] = convertDate(row[el]);
    }

    if (typeof(row[el]) === 'function') {
      newRow[el] = await convertToBuffer(row[el]);
    }
  }

  return newRow;
}

async function convertToBuffer(blobFunction: BlobFunction): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    blobFunction((err, name, e) => {
      if (err) {
        reject(err);
      } else {
        const dataChunks = [];
        e.on('data', (data) => {
          dataChunks.push(data);
        });

        e.on('end', () => {
          resolve(Buffer.concat(dataChunks));
        });

        e.on('error', (err) => {
          reject(err);
        });
      }
    });
  });
}

type BlobCallbackFunction = (err: Error | undefined, name: string, e: any) => void;
type BlobFunction = (callback: BlobCallbackFunction) => void;

interface FirebirdConnectionPool extends Firebird.ConnectionPool {
  max: number;
  dbinuse: number;
}

exitHook(() => {
  pool.destroy();
})
