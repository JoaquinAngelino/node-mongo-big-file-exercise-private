const fs = require('fs');
const csv = require('csv-parser');
const Records = require('./records.model');

/* Por performance "lean: true"; evitamos la validación si confiamos en el archivo. */
/* CONSTANTS */
const insertOptions = Object.freeze({ lean: true });
const ROWS_LIMIT = 200_000;
/* CONSTANTS */

const upload = async (req, res) => {
    const { file } = req;

    if (!file) { return res.status(400).json({ message: 'file missing' }); }

    try {
        await new Promise((resolve) => {
            let dataArray = [];
            const stream = fs.createReadStream(file.path).pipe(csv());

            stream.on('data', async (data) => {
                dataArray.push(data);

                /* Insertamos data en cantidades limitadas
                para evitar 'heap limit' error, en archivos grandes */
                if (dataArray.length === ROWS_LIMIT) {
                    stream.pause();
                    await Records.insertMany(dataArray, insertOptions);
                    dataArray = [];
                    stream.resume();
                }
            });

            stream.on('end', async () => {
                await Records.insertMany(dataArray, insertOptions);
                resolve();
            });

            stream.on('error', (err) => { throw err; });
        });

        return res.status(200).json({ message: 'Ok' });
    } catch (error) {
        return res.status(400).json({ message: error.message });
    } finally {
        /* Removemos el archivo sin importar el resultado */
        fs.unlink(file.path, (err) => { if (err) { console.log(err); } });
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
