const fs = require('fs');
const util = require('util');
const zlib = require('zlib');
const csv = require('fast-csv');
const { format } = require('date-fns');

const readdir = util.promisify(fs.readdir);
const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);


var profile = {};
const dp = '/home/julian/Desktop/sncb_realtime/';
const dataFile = '/home/julian/Desktop/rt_analysis.json';

async function count() {
    let rt_lcs = await readdir(dp);
    for (let u of rt_lcs) {
        let cxs = (await readAndGunzip(dp + u)).split('\n');
        for (let i in cxs) {
            let cx = JSON.parse(cxs[i]);
            if (!profile[cx['mementoVersion']]) {
                profile[cx['mementoVersion']] = {
                    'cxCount': 1,
                    'minTime': cx['departureTime'],
                    'maxTime': cx['arrivalTime']
                };
            } else {
                profile[cx['mementoVersion']]['cxCount']++;
                if (profile[cx['mementoVersion']]['minTime'] > cx['departureTime']) {
                    profile[cx['mementoVersion']]['minTime'] = cx['departureTime']
                }
                if (profile[cx['mementoVersion']]['maxTime'] < cx['arrivalTime']) {
                    profile[cx['mementoVersion']]['maxTime'] = cx['arrivalTime']
                }
            }
        }
        process.stdout.write('Processing live update ' + u + '\r');
    }
    await writeFile(dataFile, JSON.stringify(profile), 'utf8');
}

async function getConnectionsPerUpdate() {
    const november = '/home/julian/Desktop/november_data/';
    const november_lc = '/home/julian/Desktop/november_lc/';

    let fragments = await readdir(november);
    for (let f of fragments) {
        let cxs = (await readAndGunzip(november + f)).split('\n');
        for (let i in cxs) {
            let memento = JSON.parse(cxs[i])['mementoVersion'];
            fs.appendFileSync(november_lc + memento + '.json', cxs[i] + '\n', 'utf8', err => {
                if (err) throw err;
            });
        }
    }
}

async function analyze() {
    let data = JSON.parse(await readFile(dataFile));
    let timeStamps = Object.keys(data);
    let maxCxs = 0;
    let maxCxsTime = '';
    let avgCxs = 0;
    let avgTime = 0;
    let cxsPerDay = {};
    let cxsPerHour = {};

    for (let i in timeStamps) {
        // Process only November 2019 (Brussels time zone)
        let updTime = new Date(timeStamps[i]);
        if (updTime >= new Date('2019-10-31T23:00:00.000Z') && updTime < new Date('2019-11-30T23:00:00.000Z')) {
            // Find the biggest update
            if (maxCxs < data[timeStamps[i]]['cxCount']) {
                maxCxs = data[timeStamps[i]]['cxCount'];
                maxCxsTime = updTime.toISOString();
            }
            // Calculate average amount of Connections
            avgCxs += data[timeStamps[i]]['cxCount'];

            // Calculate average time window
            let maxTime = new Date(data[timeStamps[i]]['maxTime']);
            let minTime = new Date(data[timeStamps[i]]['minTime']);
            avgTime += maxTime.getTime() - minTime.getTime();

            // Get amount of connections per day
            let currentDay = format(updTime, 'yyyy-MM-dd');
            if (!cxsPerDay[currentDay]) {
                cxsPerDay[currentDay] = data[timeStamps[i]]['cxCount'];
            } else {
                cxsPerDay[currentDay] += data[timeStamps[i]]['cxCount']
            }

            // Get amount of connections per hour
            let currentHour = format(updTime, 'yyyy-MM-dd\'T\'HH');
            if (!cxsPerHour[currentHour]) {
                cxsPerHour[currentHour] = data[timeStamps[i]]['cxCount'];
            } else {
                cxsPerHour[currentHour] += data[timeStamps[i]]['cxCount']
            }
        }
    }

    avgCxs = avgCxs / timeStamps.length;
    avgTime = (avgTime / timeStamps.length) / (1000 * 60 * 60);

    console.log('Maximum amount of Connections:', maxCxs, '(happened on ' + maxCxsTime + ')');
    console.log('Average amount of Connections:', avgCxs);
    console.log('Average update time window (hours):', avgTime);
    //console.log('Amount of updated Connections per day:', cxsPerDay);
    //console.log('Amount of updated Connections per hour:', cxsPerHour);

    console.log(JSON.stringify(Object.values(cxsPerHour)));
}

function serverCPU() {
    return new Promise((resolve, reject) => {
        let y_cpu = [];
        fs.createReadStream('../results/server/polling-server/cpu.csv', { encoding: 'utf8', objectMode: true })
            //fs.createReadStream('../results/server/pushing-server/cpu.csv', { encoding: 'utf8', objectMode: true })
            .pipe(csv.parse({ objectMode: true, headers: false }))
            .on('data', data => {
                y_cpu.push(parseInt(data[1]));
            }).on('finish', () => {
                y_cpu.shift();
                console.log(y_cpu + '');
                resolve();
            });
    });
}

function serverRAM() {
    return new Promise((resolve, reject) => {
        let y_ram = [];
        //fs.createReadStream('../results/server/polling-server/mem.csv', { encoding: 'utf8', objectMode: true })
        fs.createReadStream('../results/server/pushing-server/mem.csv', { encoding: 'utf8', objectMode: true })
            .pipe(csv.parse({ objectMode: true, headers: false }))
            .on('data', data => {
                y_ram.push(parseInt(data[1]) * 120);
            }).on('finish', () => {
                y_ram.shift();
                console.log(y_ram + '');
                resolve();
            });
    });
}

async function serverPollingBandwidth() {
    let nrx = (await readFile('../results/server/polling-server/network.rx.csv', 'utf8')).split('\n');
    let ntx = (await readFile('../results/server/polling-server/network.tx.csv', 'utf8')).split('\n');
    nrx.shift();
    ntx.shift();
    let x_time = [];
    let y_bwd = [];
    let agg = 0;

    for (let i = 0; i < nrx.length - 1; i++) {
        x_time.push(i);
        agg += (parseInt((nrx[i].split(','))[1]) + parseInt((ntx[i].split(','))[1])) / 1000000;
        y_bwd.push(parseFloat(agg.toFixed(2)));
    }

    //console.log(x_time + '');
    console.log(y_bwd + '');
}

async function serverPushingBandwidth() {
    let network = (await readFile('../results/server/pushing-server/network_grep.txt', 'utf8')).split('\n');
    let x_time = [];
    let y_bwd = [];
    let x = 0;
    let agg = 0;

    for (let i = 2; i < network.length - 1; i = i + 2) {
        x_time.push(x++);
        let rx = parseInt((network[i].split(' '))[6]) - parseInt((network[i - 2].split(' '))[6]);
        let tx = parseInt((network[i + 1].split(' '))[6]) - parseInt((network[i - 1].split(' '))[6]);
        agg += (tx + rx) / 1000000;
        y_bwd.push(parseFloat(agg.toFixed(2)));
    }

    //console.log(x_time + '');
    console.log(y_bwd + '');
}

async function clientCPU() {
    let approach = 'Polling' // Polling, Pushing, Reference 
    let root = '../results/client/';
    let cxs = await readdir(root);
    let allSources = [];
    let y = [];

    for (let c of cxs) {
        let path = root + c + '/CPU/' + approach;
        let sources = (await readdir(path)).filter(s => s.endsWith('.csv'));

        for (let s of sources) {
            allSources.push((await readFile(path + '/' + s, 'utf8')).split('\n'));
        }

    }

    for (let i in allSources) {
        let src = allSources[i];
        src.shift();
        src.pop();

        for (let j = 0; j < src.length - 1; j++) {
            let s = parseInt(src[j].split(',')[1]);
            if (y[j]) {
                y[j] += s;
            } else {
                y[j] = s;
            }

        }
    }


    y = y.map(el => parseInt(el / allSources.length));
    console.log(y + '');
}

async function clientRAM() {
    let approach = 'Reference' // Polling, Pushing, Reference 
    let root = '../results/client/';
    let cxs = await readdir(root);
    let allSources = [];
    let y = [];

    for (let c of cxs) {
        let path = root + c + '/RAM/' + approach;
        let sources = (await readdir(path)).filter(s => s.endsWith('.csv'));

        for (let s of sources) {
            allSources.push((await readFile(path + '/' + s, 'utf8')).split('\n'));
        }

    }

    for (let i in allSources) {
        let src = allSources[i];
        src.shift();
        src.pop();

        for (let j = 0; j < src.length - 1; j++) {
            let s = parseFloat(src[j].split(',')[1]) * 120;
            if (y[j]) {
                y[j] += s;
            } else {
                y[j] = s;
            }

        }
    }


    y = y.map(el => parseFloat((el / allSources.length).toFixed(2)));

    console.log(y + '');
}

async function clientBandwidth() {
    let approach = 'Reference' // Polling, Pushing, Reference 
    let root = '../results/client/';
    let cxs = await readdir(root);
    let allSources = [];
    let y = [];
    let x = [];

    for (let c of cxs) {
        let path = root + c + '/Bandwidth/' + approach;
        let sources = (await readdir(path)).filter(s => s.endsWith('.csv'));
        console.log(sources);

        for (let s of sources) {
            allSources.push((await readFile(path + '/' + s, 'utf8')).split('\n'));
        }

    }

    for (let i in allSources) {
        let src = allSources[i];
        src.shift();
        src.pop();

        for (let j = 0; j < src.length - 1; j++) {
            let s = parseInt(src[j].split(',')[1]);
            if (y[j]) {
                y[j] += s;
            } else {
                y[j] = s;
            }

        }
    }


    y = y.map(el => parseInt(el / (allSources.length / 2)) / 1000);
    console.log(y + '');

    for (let k in y) {
        x.push(k);
    }
    //console.log(x + '');
}

async function routePlanning() {
    let approach = 'Rollback/'; // Rollback/, NoRollback/
    let connections = [2, 5, 6, 11, 18, 23];
    let y = [];
    let agg = 0;

    //for (let cx of connections) {
        let root = '../results/client/' + 2 + '/Query\ Performance/' + approach;
        let cxs = (await readdir(root)).filter(s => s.endsWith('.csv'));;

        let allSources = [];

        for (let c of cxs) {
            let raw = (await readFile(root + '/' + c, 'utf8')).split('\n');
            raw.shift();
            raw.shift();
            let filtered = [];

            for (let r in raw) {
                let rs = raw[r].split(',');
                if (rs[1] && !isNaN(parseInt(rs[1]))) {
                    filtered.push(parseInt(rs[1]));
                }
            }

            allSources = allSources.concat(filtered);
        }

        console.log(allSources);

        for (let i in allSources) {
            agg += allSources[i];
        }
        y.push(parseFloat((agg / allSources.length).toFixed(2)));
    //}

    console.log(y);
}

function readAndGunzip(path) {
    return new Promise((resolve, reject) => {
        let buffers = [];
        fs.createReadStream(path)
            .pipe(new zlib.createGunzip())
            .on('error', err => {
                reject(err + ' - ocurred on file: ' + path);
            })
            .on('data', data => {
                buffers.push(data);
            })
            .on('end', () => {
                resolve(buffers.join(''));
            });
    });
}

//count();
//getConnectionsPerUpdate();
//analyze();
//serverCPU();
//serverRAM();
//serverPollingBandwidth();
//serverPushingBandwidth();
//clientCPU();
//clientRAM();
//clientBandwidth();
routePlanning();