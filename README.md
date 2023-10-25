# cumulocity-mastr-wec-import
Script to import wind turbines from CSV export from https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/ErweiterteOeffentlicheEinheitenuebersicht

# How to run the scirpt

## Clone the repository

## Install
  
`npm install`

## Expoert an CSV

Go to 

https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/ErweiterteOeffentlicheEinheitenuebersicht

Configure your filter with following default filter attributes:

* Betriebs-Status entspricht In Betrieb
* Energieträger entspricht Wind
* Koordinate: Breitengrad (WGS84) ist nicht leer
* Name des Windparks is nicht leer

Custom attributes (here you can select by region etc.):
* Gemeinde entspricht *
* Postleitzahl entspricht *

![ErweiterteOeffentlicheEinheitenuebersicht](./doc/mastr-1.PNG)

It is important that the csv file is in UTF-8. Please change the encoding if necessary.

The csv file can be copied to import folder and should be changed int the app.js

```javascript
function processCSVFile() {
    fs.createReadStream('import/wec_sachsen_20231022.csv')

```

## Run the srcipt

`npm start`
