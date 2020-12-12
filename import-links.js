const r = require("rethinkdbdash")();
const { Pool } = require("pg");
const TABLE_NAME = "users";

const PG_TABLE = "links";
const pool = new Pool();

function backupTable () {
        const prom = [];
        r.db('main').table(TABLE_NAME).toStream()
            .on('error', console.error)
            .on('data', async function (d) {
                // Insert into
                const res = await pool.query(`SELECT * FROM ${PG_TABLE} WHERE discord_id = $1;`, [d.discordId]);
                if (res.rows.length === 0 && d.robloxId) {
                    const f = pool.query(`INSERT INTO ${PG_TABLE} (roblox_id, discord_id, opt_out) VALUES ($1, $2, $3)`,
                        [d.robloxId, d.discordId, false]);
                    prom.push(f);
                    console.log(`Inserted ${d.discordId}`);
                }
            })
            .on('end', async function () {
                console.log(`Stream done!`);
                prom.map((i)=>i.catch(e=> {
                    console.log(e.message);
                    return e;
                }));
                await Promise.all(prom);
                console.log(`Done!`);
            })
}
backupTable()
