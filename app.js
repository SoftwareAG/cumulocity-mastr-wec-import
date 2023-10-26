const fs = require('fs');
const csv = require('csv-parser');
const c8y = require('./c8y')

/**
 * Load environment variables
 */
const result = require('dotenv').config();
if (result.error) {
    throw result.error
}
console.log('--- Environment loaded! ---');

const c8yConnectionConf = {
    baseUrl: process.env.C8Y_BASEURL,
    user: process.env.C8Y_USER,
    tenant: process.env.C8Y_TENANT,
    password: process.env.C8Y_PASSWORD
}

const results = [];

const territories = new Map();

const windFarms = new Map();

// Define the custom delimiter (semicolon in this case)
const customDelimiter = ';';

console.log('STARTING STEP1: init C8y Client')
c8y.initClient(c8yConnectionConf).then(result => {
    if (result === true) {
        console.log('init c8y client successful!');
        processCSVFile();
    } else {
        console.log('init c8y client failed!');
    }
});

function processCSVFile() {
    fs.createReadStream('import/wec_rostock_20231026.csv')
        .pipe(csv({ separator: customDelimiter }))
        .on('data', (row) => {
            //console.log(row);
            const mastNrValue = row['MaStR-Nr. der Einheit'];
            const nameValue = row['Anzeige-Name der Einheit'];
            const nominalPowerValue = Number(row['Nettonennleistung der Einheit']);
            const commissioningDate = row['Inbetriebnahmedatum der Einheit'];
            const cityValue = row['Ort'];
            const cityCodeValue = row['Postleitzahl'];
            const streetValue = row['Straße'] + row['Hausnummer'];
            const territoryValue = row['Bundesland'];
            //const latitudeValue = row['Koordinate: Breitengrad (WGS84)'];
            const latitudeValue = parseFloat(row['Koordinate: Breitengrad (WGS84)'].replace(',', '.'));
            const longitudeValue = parseFloat(row['Koordinate: Längengrad (WGS84)'].replace(',', '.'));
            const windFarmNameValue = row['Name des Windparks'];
            const hubHeightValue = Number(row['Nabenhöhe der Windenergieanlage']);
            const rotorDiameterValue = Number(row['Rotordurchmesser der Windenergieanlage']);
            const manufacturerValue = row['Hersteller der Windenergieanlage'];
            const typeValue = row['Typenbezeichnung'];
            const operatorNameValue = row['Name des Anlagenbetreibers (nur Org.)'];
            const locationOfWec = row['Lage der Einheit'];
            const wec_GridOperator = row['Name des Anschluss-Netzbetreibers'];

            const wec = {
                c8y_IsAsset: {},
                c8y_IsDeviceGroup: {},
                wec_MaStRNr: mastNrValue,
                wec_Serial: mastNrValue,
                name: nameValue,
                wec_NominalPowerKW: nominalPowerValue,
                wec_CommissioningDate: commissioningDate,
                wec_Address: {
                    street: streetValue,
                    city: cityValue,
                    cityCode: cityCodeValue,
                    country: "Germany",
                    territory: territoryValue
                },
                c8y_Position: {
                    lat: latitudeValue,
                    lng: longitudeValue
                },
                wec_Model: typeValue,
                wec_WindFarmName: windFarmNameValue,
                wec_HubHeightM: hubHeightValue,
                wec_RotorDiameterM: rotorDiameterValue,
                wec_Manufacturer: manufacturerValue,
                type: "windTurbine",
                wec_Operator: operatorNameValue,
                wec_GridOperator: wec_GridOperator,
                wec_IsOnshore: "Windkraft an Land" === locationOfWec,
                wec_IsOffshore: "Windkraft auf See" === locationOfWec      
            }

            putWindFarm(windFarmNameValue, territoryValue, nominalPowerValue);

            putTerritory(territoryValue, nominalPowerValue);

            results.push(wec);
        })
        .on('end', () => {           
            processTerritory().then(result => {
                console.log('Territories processed!');
                processWindFarm().then(result => {
                    console.log('Windfarms processed!');
                    processWindTurbine();
                });
            });
        });
}

async function processTerritory() {
    console.log(`Process territories, size: %d`, territories.size);

    for (const [key, value] of territories.entries()) {
        try {
            const territory = await c8y.getManagedObjectByTypeAndName(value.type, value.name);
            console.log(`Territory already exists: ${territory.id}`);
            territories.set(key, territory);
        }catch(error) {
            console.log(`Territory doesn't exists, error: ${JSON.stringify(error)}`);
            try {
                const territory = await c8y.createManagedObject(value);
                territories.set(key, territory.data);
            }catch(error) {
                console.log(`Territory couldn't created: ${JSON.stringify(error)}`);
            }

        }

    }
}


async function processWindFarm() {
    console.log(`Proces Windfarms, size: %d`, windFarms.size);

    for (const [key, value] of windFarms.entries()) {
        try {
            const windFarm = await c8y.getManagedObjectByTypeAndName(value.type, value.name);
            console.log(`Windfarm already exists: ${windFarm.id}`);
            windFarms.set(key, windFarm);
        }catch(error) {
            console.log(`Windfarm doesn't exist, error: ${JSON.stringify(error)}`);
            const territory = territories.get(value.wec_TerritoryName);
            try {
                const windFarm = await c8y.createManagedObjectWithParent(value, territory.id);
                windFarms.set(key, windFarm.data);
            }catch(error) {
                console.log(`Windfarm couldn't be created: ${JSON.stringify(error)}`);
            }
        }

    }
}

async function processWindTurbine() {
    console.log(`Process wind turbines, size: %d`, results.length);

    for (const value of results) {
        try {
            const windTurbine = await c8y.getManagedObjectByExternalId("wec_MaStRNr", value.wec_MaStRNr);
            console.log(`Device already exists: ${windTurbine.data.id}`);
        } catch (error) {
            console.log(`Device doesn't exists, error: ${JSON.stringify(error)}`);
            const windFarm = windFarms.get(value.wec_WindFarmName);
            try {
                const windTurbine = await c8y.createDeviceAndExternalId(value, value.wec_MaStRNr, windFarm.id);
                console.log(`Device created: ${windTurbine.data.id}`);
            }catch(error) {
                console.log(`Device couldn't created: ${JSON.stringify(error)}`);
            }
        }
    }
}

function putTerritory(territoryName, nominalPowerValue) {
    if (territories.has(territoryName)) {
        var territory = territories.get(territoryName);
        territory.wec_NominalPowerTotal += nominalPowerValue;
    } else {
        const territory = {
            name: territoryName,
            c8y_IsAsset: {},
            c8y_IsDeviceGroup: {},
            type: "territory",
            icon: {
                "category": "location",
                "name": "location"
            },
            wec_NominalPowerTotal: nominalPowerValue
        }
        territories.set(territoryName, territory);
    }
}

function putWindFarm(windFarmName, territoryName, nominalPowerValue) {
    if (windFarms.has(windFarmName)) {
        var windFarm = windFarms.get(windFarmName);
        windFarm.wec_NominalPowerTotal += nominalPowerValue;
    } else {
        const windFarm = {
            name: windFarmName,
            c8y_IsAsset: {},
            c8y_IsDeviceGroup: {},
            type: "windFarm",
            icon: {
                "category": "location",
                "name": "address"
            },
            wec_NominalPowerTotal: nominalPowerValue,
            wec_TerritoryName: territoryName
        }
        windFarms.set(windFarmName, windFarm);
    }
}
