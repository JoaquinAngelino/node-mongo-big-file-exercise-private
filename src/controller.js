const fs = require('fs');
const csv = require('csv-parser');
const Records = require('./records.model');

/* parallelInsert: divide la carga de trabajo en 5 hilos o procesos,
mejora la performance de inserción al realizar varias operaciones al mismo tiempo, */
const parallelInsert = async (docsArray) => {
    const promiseArray = [];
    const SLICE = Math.ceil(docsArray.length / 5);
    const insertOptions = { lean: true, ordered: false };

    for (let i = 0; i < 5; i += 1) {
        const docs = docsArray.slice(SLICE * i, SLICE * (i + 1));
        promiseArray.push(Records.insertMany(docs, insertOptions));
    }

    await Promise.all(promiseArray);
};

const upload = async (req, res) => {
    const { file } = req;

    if (!file) { return res.status(400).json({ message: 'File missing' }); }

    try {
        await new Promise((resolve) => {
            const MAX_DOCS = 100_000;
            const stream = fs.createReadStream(file.path).pipe(csv());
            let docsArray = [];

            stream.on('data', async (data) => {
                docsArray.push(data);
                /* Por escalabilidad procesamos datos en cantidades limitadas para evitar el error
        'heap limit' en archivos grandes; Ajustable si se necesita trabajar usando menos memoria. */
                if (docsArray.length === MAX_DOCS) {
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
        /* Eliminamos el archivo independientemente del resultado usando la cláusula finally. */
        fs.unlink(file.path, () => { });
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
