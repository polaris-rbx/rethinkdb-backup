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
const { stat, mkdir, createWriteStream, readdir, readFile} = require("fs");


const backupBase = join(__dirname, "backup");

async function findBase () {
    const files = await toPromise(readdir, backupBase, {
        withFileTypes: true
    });
    let mostRecent = "";
    for (let file of files) {
        if (file.isDirectory()) {
            // "" doesn't eval to greater than
            if (mostRecent < file.name) {
                // Update most recent
                mostRecent = file.name;
            }
        }
    }
    if (mostRecent === "") {
        console.error("Failed to find suitable folder for restore - No folders found!");
        process.exit(1);
        return false;
    }
    return join(backupBase, mostRecent);
}

async function restore () {
    const path = await findBase();
    if (!path) return;
    const files = await toPromise(readdir, path, {
        withFileTypes: true
    });
    const databases = await r.dbList();
    for (let file of files) {
        if (file.isDirectory()) {
            // Check DB exists
            // ATM we **will** create the database but in future this something I would want to make configurable
            // Given it's pretty "dangerous"
            const dbName = file.name;
            if (!databases.includes(dbName)) {
                console.log(`No DB exists for ${dbName}, creating one.`);
                await r.dbCreate(dbName);
            }

            // Restore tables
            const tableFiles = await toPromise(readdir, join(path, dbName), {
                withFileTypes: true
            });
            const tables = await r.db(dbName).tableList();
            for (let tableFile of tableFiles) {
                try {
                    if (tableFile.isFile()) {
                        const tableName = tableFile.name.split(".")[0];
                        console.log(`Backing up ${dbName}: ${tableName}`);
                        if (!tables.includes(tableName)) {
                            console.log(`No table exists for ${dbName}: ${tableName}, creating one.`);
                            await r.db(dbName).tableCreate(tableName);
                        }

                        let raw = await toPromise(readFile, join(path, dbName, tableFile.name), {
                            encoding: "utf8"
                        });

                        if (typeof raw !== "string") {
                            throw new Error("File contents are not a string!");
                        }
                        const parsed = JSON.parse(raw);
                        // Potentially big files, so save memory by removing it
                        raw = "";
                        const promises = [];
                        for (let item of parsed) {
                            // Actual import logic
                            const id = item.id ? item.id : item.discordId;
                            const prom  = r.db(dbName).table(tableName).get(id)
                                .then(function (res){
                                    if (res) {
                                        return r.db(dbName).table(tableName).update(item);
                                    } else {
                                        return r.db(dbName).table(tableName).insert(item);
                                    }
                            })
                                .catch(function (e) {
                                    console.error(`${dbName}: ${tableName} Failed to restore row. Error: ${e.message}\n${e.stack}`)
                                });
                            promises.push(prom);
                        }
                        await Promise.all(promises);
                        console.log(`Restored table ${tableName}`);
                    }
                } catch (e) {
                    console.error(`Failed to restore table. Error: ${e.message}\n\n${e.stack}`);


                }
            }


        }
    }
}

function toPromise(func, ...args) {
    return new Promise(function (resolve, reject) {
        func(...args, function (error, ...resp) {
            if (error) {
                return reject(error);
            }
            resolve(...resp);
        })
    });
}

// Main
restore()
    .catch(function (e) {
        console.log(`Failed to back up and error was not handled.\nError: ${e.message}\n${e.stack}`);
    })