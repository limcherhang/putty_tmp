const { exec } = require('child_process');
const { MongoClient } = require('mongodb');

const url = "mongodb://carbonAdmin:carbonAdmin%23139@20.6.34.160:27017/db_prod_v1";

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
    if (err) {
        console.error('MongoDB Connection Error', err);
        return;
    }
    
    console.log('MongoDB Connection Success');

    const db = client.db(); // 获取默认数据库，您在连接URL中已经指定了数据库名称，因此不需要再传递dbName参数
    const site_modules = db.collection('site_modules');

    site_modules.find({}).toArray((err, modules) => {
        if (err) {
            console.error('MongoDB Find Error', err);
            return;
        }

        console.log('Site Modules:', modules);

        // 在查询结果返回后关闭 MongoDB 连接
        client.close();
    });
});

// const pythonProcess = exec('cd ./python_scripts && python3 ghg_report.py 65b9b22e3bb61226a5bc5d4c 2022 dev', (error, stdout, stderr) => {
//     if (error) {
//         console.error(`Execute Error: ${error}`);
//         return;
//     }
//     console.log(`Python script output: ${stdout}`);
// });

// pythonProcess.stdout.on('data', (data) => {
//     console.log(`Python script output: ${data}`);
// });

// pythonProcess.stderr.on('data', (data) => {
//     console.error(`Python script error: ${data}`);
// });