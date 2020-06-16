/*
Exports all databases and tables. Stores in folders according to to "date" in a similar format to npm logs
Dates are UTC but do not contain the timezone offset.
Uses streams to try and avoid storing massive tables in memory, but the way this is done is a bit "Hacky" so be aware of that.
    (I manually write commas and array [] parts)
    By default ignores the "test" db.

 Dependencies:
  * rethinkdbdash
*/
// At a later date I'll make it read these in from passed values
const blacklist = ["rethinkdb"];

const r = require("rethinkdbdash")();
const { join } = require("path");
const {stat, mkdir, createWriteStream} = require("fs");


const d = new Date();
const dateStr = d.toJSON().replace(/:/g, "_").split(".")[0];

const backupBase = join(__dirname, "backup", dateStr);


async function backup () {
    await ensureExits(backupBase);
    const databases = await r.dbList();
    for (let db of databases) {
        if (!blacklist.includes(db)) {
            const tables = await r.db(db).tableList();
            // Backup each
            let passed = 0;
            let failed = 0;
            for (let table of tables) {
                try {
                    const resp = await backupTable(db, table);
                    if (resp) {
                        passed++
                        console.log(`Backed up ${table}`);
                    }
                } catch (e) {
                    console.error(`Failed to backup table ${table}. Error: ${e.message}\n\n${e.stack}`);
                    failed++

                }
            }
            console.log(`${db} Backup Finished. ${passed} passes, ${failed} fails.`)
        }

    }
}
function backupTable (db, tableName) {
    return new Promise(function (resolve, reject) {

        // Ensure folder exists
        ensureExits(join(backupBase, db))
            .then(function () {
                const str = join(backupBase, db, tableName);
                const file = createWriteStream(`${str}.json`);
                file.on('error', reject);
                file.write("[")
                let isFirst = true;
                function handleErr (err) {
                    file.write("]");
                    file.close();
                    reject(err);
                }
                r.db(db).table(tableName).toStream()
                    .on('error', handleErr)
                    .on('data', function (d) {
                        const val = JSON.stringify(d, null, 2);
                        file.write(`${isFirst ? "" : ",\n"}${val}`)
                        isFirst = false;
                    })
                    .on('end', function () {
                        file.write("\n]")
                        file.end()
                        resolve(true);
                    })

            })
            .catch(reject)
    })

}

function ensureExits (path) {
    return new Promise(function (resolve, reject) {
        stat(path, function (err, s) {
            if (err) {
                if (err.code === "ENOENT") {
                    mkdir(path, {
                        recursive: true
                    }, function (err) {
                        if (err) reject(err);
                        resolve(true);
                    });
                } else {
                    console.log(err.code)
                    reject(err);
                }
            } else {
                resolve(true);
            }

        });
    })
}
backup();