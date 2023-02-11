const fs = require('fs');
const csv = require('csv-parser');
const Records = require('./records.model');

// Por performance "lean: true"; evitamos la validación si confiamos en el archivo.
const insertOptions = { lean: true };
const ROWS = 200_000;

const upload = async (req, res) => {
    const { file } = req;

    if (!file) { return res.status(400).json({ message: 'Bad request: file missing' }); }

    try {
        await new Promise((resolve) => {
            let arr = [];
            const stream = fs.createReadStream(file.path).pipe(csv());

            stream.on('data', async (data) => {
                arr.push(data);

                /* Insertamos data en cantidades limitadas
                para evitar 'heap limit' error, en archivos grandes */
                if (arr.length === ROWS) {
                    stream.pause();
                    await Records.insertMany(arr, insertOptions);
                    arr = [];
                    stream.resume();
                }
            });

            stream.on('end', async () => {
                await Records.insertMany(arr, insertOptions);
                resolve();
            });

            stream.on('error', (err) => { throw err; });
        });

        return res.status(200).json({ message: 'OK' });
    } catch (error) {
        return res.status(400).json({ message: error.message });
    } finally {
        // removemos el archivo sin importar el resultado
        fs.unlink(file.path, (err) => { if (err) throw err; });
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
