#!/usr/bin/env node
const fs = require("fs").promises;
const parse = require("csv-parse/lib/sync");
const axios = require("axios").default;

async function openCSV() {
  let file = [];
  file = await fs.readFile("data.csv", "utf8");

  var dataArray = file.split(/\r?\n/);

  const records = parse(file, {
    columns: true,
    skip_empty_lines: true,
  });
  return records;
}

async function hitApis(rows) {
  let completedhouseNumber = "";
  let compltedPostCode = "";
  let compltedHouseLetter = "";
  let badPostCodes = ["", "0000aa", ".", "zipcode", "N.v.t.", "0000CW"];
  let badHouseNumbers = [
    "9999",
    "",
    ".",
    "house",
    "Omloop",
    "nirwana",
    "Gasgracht",
    "Ruwenbos",
    "Ool",
    "hoven",
    "Vinkwijkse",
  ];
  // let objectIndexToGet = 0;

  for (let i = 0; i < rows.length; i++) {
    let currentRow = rows[i];

    let currentPostCode = currentRow.POSTCODE;
    currentPostCode = currentPostCode.replace(" ", "");
    let currentHouseNumber = currentRow.Huisnummer;
    let currentHouseLetter = currentRow.HuisnummerExtra ? currentRow.HuisnummerExtra : null;
    if (currentHouseLetter === "0" || currentHouseLetter === "1" || currentHouseLetter?.indexOf("-") >= 0) {
      currentRow.Object = "Skipped checking because Multiple or bad House Letters";
      continue;
    }

    const badInput = badPostCodes.indexOf(currentPostCode) >= 0 || badHouseNumbers.indexOf(currentHouseNumber) >= 0;
    const samePostAndNum = completedhouseNumber === currentHouseNumber && compltedPostCode === currentPostCode;
    const letterExists = currentHouseLetter !== null;
    const sameLetter = compltedHouseLetter === currentHouseLetter;

    if (badInput || samePostAndNum) {
      if (letterExists && sameLetter) {
        currentRow.Object = "Skipping because bad input or same Postcode and Number and letter from previous row.";
        continue;
      } else if (!letterExists) {
        currentRow.Object = "Skipping because bad input or same Postcode and Number from previous row.";
        continue;
      }
    }

    completedhouseNumber = currentHouseNumber;
    compltedPostCode = currentPostCode;
    compltedHouseLetter = currentHouseLetter;

    const houseNumber = await filterRange(currentHouseNumber);
    let houseLetter = null;
    if (currentHouseLetter !== null) {
      houseLetter = await filterRange(currentHouseLetter);
    }

    if (houseNumber == null) {
      currentRow.Object = "Skipped checking because Multiple House Numbers";
      console.log("Skipped checking because Multiple House Numbers");
    } else if (currentHouseLetter !== null && houseLetter == null) {
      currentRow.Object = "Skipped checking because Multiple House Letters";
      console.log("Skipped checking because Multiple House Letters");
    } else {
      const objectCode = await callFistAPI(currentPostCode, houseNumber, currentHouseLetter);

      if (objectCode.code !== null) {
        const secondAPIResponse = await hitSecondAPI(objectCode);

        if (secondAPIResponse === undefined) {
          currentRow.Object = "Second API return nothing";
          continue;
        }

        let propertyArea = secondAPIResponse.oppervlakte;

        currentRow.Oppervlakte = propertyArea;
        console.log(`PropertyArea ${propertyArea}`);

        let arrayOfPropertyType = secondAPIResponse.gebruiksdoel;

        if (arrayOfPropertyType.length === 1) {
          rows[i].gebruiksdoel1 = arrayOfPropertyType[0];
          console.log(`building type: ${arrayOfPropertyType[0]}`);
        } else if (arrayOfPropertyType.length === 2) {
          currentRow.gebruiksdoel1 = arrayOfPropertyType[0];
          currentRow.gebruiksdoel2 = arrayOfPropertyType[1];

          console.log(`building type: ${arrayOfPropertyType[0]}, ${arrayOfPropertyType[1]}`);
        } else {
          currentRow.gebruiksdoel1 = arrayOfPropertyType[0];
          currentRow.gebruiksdoel2 = arrayOfPropertyType[1];
          currentRow.gebruiksdoel3 = arrayOfPropertyType[2];
          console.log(`building type: ${arrayOfPropertyType[0]}, ${arrayOfPropertyType[1]}, ${arrayOfPropertyType[2]}`);
        }
      } else {
        currentRow.Object = objectCode.error;
      }
    }
  }
}

async function filterRange(string) {
  if (string.indexOf("-") >= 0) {
    return null;
  } else {
    return string;
  }
}

async function callFistAPI(postCode, houseNumber, houseLetter) {
  const urlWithParams = `https://bag.basisregistraties.overheid.nl/api/v1/nummeraanduidingen?postcode=${postCode}&huisnummer=${houseNumber}&huisletter=${houseLetter}`;
  const urlWithParamsNoLetter = `https://bag.basisregistraties.overheid.nl/api/v1/nummeraanduidingen?postcode=${postCode}&huisnummer=${houseNumber}`;
  const url = houseLetter === null ? urlWithParamsNoLetter : urlWithParams;
  console.log(url);
  let firstApiResult;
  try {
    const response = await axios({
      url: url,
      method: "get",
      headers: {
        "Accept-Crs": "epsg:28992",
        "X-Api-Key": "b2ea5197-dbfd-4649-9c71-c63c99ca59ca",
        "Accept-Encoding": "gzip, deflate, br",
        Accept: "*/*",
      },
    });
    firstApiResult = response.data;
  } catch (err) {
    console.log(`Fist API had error: ${err?.data?.detail}`);
    return { error: err?.data?.detail, code: null };
  }

  const firstResult = firstApiResult._embedded.nummeraanduidingen;
  let objectCode = "";

  if (firstResult.length === 0) {
    console.log("Skipping empty first API return!");
    return { error: "Skipping empty first API return!", code: null };
  } else if (firstResult.length === 1) {
    if (firstResult[0]._links.adresseerbaarObject === null) {
      return { error: "adresseerbaarObject is null on only object", code: null };
    }
    const rawLabel = firstResult[0]._links.adresseerbaarObject?.label;
    objectCode = rawLabel.split(" ")[1];
    console.log(`ObjectCode: ${objectCode}`);
    return { error: null, code: objectCode };
  } else {
    let mostRecent = null;
    let mostRecentObjectCode = null;
    firstResult.forEach((object) => {
      if (firstResult[0]._links.adresseerbaarObject === null) {
        return { error: "adresseerbaarObject is null on latest object", code: null };
      }
      const rawLabel = firstResult[0]._links.adresseerbaarObject?.label;
      objectCode = rawLabel.split(" ")[1];
      let date = Date.parse(object._embedded.geldigVoorkomen.beginGeldigheid);

      if (mostRecentObjectCode === null || mostRecent === null) {
        mostRecentObjectCode = objectCode;
        mostRecent = date;
      }
      if (mostRecent !== null && mostRecent < date) {
        mostRecent = date;
        mostRecentObjectCode = objectCode;
      } else if (mostRecent === date && object._embedded.status === "Naamgeving uitgegeven") {
        mostRecentObjectCode = objectCode;
      }
    });
    return { error: null, code: mostRecentObjectCode };
  }
}

async function hitSecondAPI(objectCode) {
  try {
    const response2 = await axios({
      url: `https://bag.basisregistraties.overheid.nl/api/v1/verblijfsobjecten/${objectCode.code}`,
      method: "get",
      headers: {
        "Accept-Crs": "epsg:28992",
        "X-Api-Key": "b2ea5197-dbfd-4649-9c71-c63c99ca59ca",
        "Accept-Encoding": "gzip, deflate, br",
        Accept: "*/*",
      },
    });

    return response2.data;
  } catch (err) {
    console.log(`failed on second api request ${err}`);
  }
}

async function exportJSON(rows) {
  const jsonOutput = JSON.stringify(rows);
  try {
    await fs.writeFile("dataOutput.json", jsonOutput);
    console.log("JSON data is saved.");
  } catch (error) {
    console.error(error);
  }
}

async function main() {
  const rows = await openCSV();
  await hitApis(rows);
  await exportJSON(rows);
}

main()
  .then(() => console.log("Done"))
  .catch((ex) => console.log(ex.message));
