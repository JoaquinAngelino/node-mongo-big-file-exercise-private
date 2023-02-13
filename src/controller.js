const fs = require('fs');
const csv = require('csv-parser');
const Records = require('./records.model');
/**
 *  1 - parallelInsert: se divide la carga de trabajo en varios hilos o procesos,
 *      lo que permite realizar varias operaciones de inserción al mismo tiempo.
 *
 *  2 - insert options: podemos configurar las opciones de inserción para extra
 *      performance evitando validaciónes/checkeos -No recomendable-.
 *
 *  3 - Leemos data en cantidades limitadas para evitar el error 'heap limit' en archivos grandes;
 *      Ajustable si se necesita trabajar usando menos memoria.
 *
 *  4 - Eliminamos el archivo independientemente del resultado usando la clausula finally.
*/

const parallelInsert = async (docsArray) => { // * 1 *
    const SLICE = 20_000;
    const promiseArray = [];
    for (let i = 1; i < 6; i += 1) {
        const docs = docsArray.slice(SLICE * (i - 1), SLICE * i);
        promiseArray.push(Records.insertMany(docs, ({ // * 2 *
            lean: true,
            ordered: false,
            // writeConcern: { w: 0, j: false },
        })));
    }
    await Promise.all(promiseArray);
};

const upload = async (req, res) => {
    const { file } = req;

    if (!file) { return res.status(400).json({ message: 'File missing' }); }

    try {
        await new Promise((resolve) => {
            let docsArray = [];
            const stream = fs.createReadStream(file.path).pipe(csv());

            stream.on('data', async (data) => {
                docsArray.push(data);
                if (docsArray.length === 100_000) { // * 3 *
                    stream.pause();
                    await parallelInsert(docsArray);
                    docsArray = [];
                    stream.resume();
                }
            });

            stream.on('end', async () => {
                await parallelInsert(docsArray);
                resolve();
            });

            stream.on('error', (err) => { throw err; });
        });

        return res.status(200).json({ message: 'Documents inserted correctly' });
    } catch (error) {
        return res.status(500).json({ message: error.message });
    } finally {
        fs.unlink(file.path, () => { }); // * 4 *
    }
};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();

        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
