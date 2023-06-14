import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import fs from 'fs'
import { availableParallelism } from 'os'
import path from 'path';
import csv from 'csv-parser'


const records = {
    start: Date.now(),
}


  process.on('exit', () => {
        console.log(`Process started ${formatDateTime(records.start)}`);
        console.log(`Process cost ${Date.now() - records.start} miliseconds`);
        delete records.start
        let overall = 0
        for (const key in records) {
           let [duration, amount] = records[key]
           console.log(`${key} took ${duration} miliseconds to parse, ${amount} amount of data has been read/written`)
           overall += amount
        }
        console.log(`overall ${overall} amount of data has been read/written`);
    })


if (isMainThread) {

    let files = fs.readdirSync(process.argv[2]).filter((file) => {
        return path.extname(file) == '.csv'
    });

    let countOfWorkers = Math.min(files.length, availableParallelism())
    let planner = separateFilesForWorkers(files, countOfWorkers)

    for (let i = 0; i < countOfWorkers; i++) {
        const worker = new Worker('./file.js');

        worker.on('online', () => {
            worker.postMessage(planner[i])
    
        });
        worker.on('message', (msg)=>{

           if(msg.done){
                // console.log(`message from  worker ${worker.id}:`);
                // console.log('JSON file has been written successfully! '); 
                records[msg.filename] = msg.duration_amount
                worker.terminate()

            }
        })
        worker.on('error', (err)=>{
           console.log(err);
        })
    
        
    }
  
  
}
else {
   
    parentPort.on('message',(data) => {
        return new Promise((resolve, reject) => {
            let all = []
            for (let i = 0; i < data.length; i++) {
                all.push(parseData(data[i]))
            }
            Promise.allSettled(all).then(e => {
                resolve(e)
            })
               .catch(err => reject(err))
        })

    }) 
}


function separateFilesForWorkers(files, workersCount) {

    const group = new Array(workersCount).fill(0).map(e => [])
    for (let i = 0; i < files.length; i++) {
        group[i % workersCount].push(files[i]);
    }
    return group;
}

function parseData(file) {
    const results = [];

    return new Promise((resolve, reject) => {

        fs.createReadStream(file)
            .pipe(csv())
            .on('data', (data) => {
                results.push(data)
            }
            )
            .on('end', () => {
                const jsonData = JSON.stringify(results);
                let dirname = process.argv[2] ?? './'
                let outputFile = path.resolve(dirname, 'converted')
             

                let fileName = `${path.basename(file, '.csv')}.json`

                if (!fs.existsSync(outputFile)) {
                    fs.mkdirSync(outputFile);
                }

                fs.writeFile(path.join(outputFile, fileName), jsonData, (err) => {
                    if (err) {
                        reject('Error writing JSON file:' + err)
                    } else {
                        parentPort.postMessage({
                            done:true, 
                            filename: fileName, 
                            duration_amount : [Date.now() - records.start, results.length]
                        })
                        resolve(results)  
                    }
                });
            })
            .on('error', (error) => {
                reject('Error writing JSON file:' + error)
            });
    })
}


function formatDateTime(timestamp) {
    const currentDate = new Date(timestamp);

    const padZero = (value) => value.toString().padStart(2, '0');

    const year = currentDate.getFullYear();
    const month = padZero(currentDate.getMonth() + 1);
    const day = padZero(currentDate.getDate());
    const hour = padZero(currentDate.getHours());
    const minutes = padZero(currentDate.getMinutes());
    const seconds = padZero(currentDate.getSeconds());
    const milliseconds = padZero(currentDate.getMilliseconds()).padStart(3, '0');

    return `${year}/${month}/${day}_${hour}:${minutes}:${seconds}:${milliseconds}`;
}


