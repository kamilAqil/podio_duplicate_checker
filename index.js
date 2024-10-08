const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Podio = require('podio-js').api;
require('dotenv').config();

const PODIO_CLIENT_ID = process.env.PODIO_CLIENT_ID;
const PODIO_CLIENT_SECRET = process.env.PODIO_CLIENT_SECRET;
const PODIO_APP_ID = process.env.PODIO_APP_ID;
const PODIO_API_TOKEN = process.env.PODIO_API_TOKEN;
const podio = new Podio({
    authType: 'app',
    clientId: PODIO_CLIENT_ID,
    clientSecret: PODIO_CLIENT_SECRET
});

// supporting functions

// Function to process all CSV files in a specified directory, passing the authenticated Podio client
async function processCSV(directoryPath, podioClient) {
    try {
        const files = fs.readdirSync(directoryPath);

        for (const file of files) {
            const filePath = path.join(directoryPath, file);
            const stat = fs.statSync(filePath);

            if (stat.isFile() && path.extname(file) === '.csv') {
                console.log(`Processing file: ${filePath}`);

                fs.createReadStream(filePath)
                    .pipe(csv())
                    .on('data', async (row) => {

                        // Check for duplicates using the specified field IDs
                        const isDuplicate = await checkForDuplicate(row, podioClient);
                        console.log('isDuplicate', isDuplicate);

                        if (!isDuplicate) {
                            await createPodioRecord(row, podioClient);
                        } else {
                            console.log(`Skipping duplicate entry for row`, row.Address);
                        }
                    })
                    .on('end', () => {
                        console.log(`Finished processing file: ${filePath}`);
                    })
                    .on('error', (err) => {
                        console.error(`Error reading file ${filePath}: ${err.message}`);
                    });
            } else {
                console.log(`Skipping non-file or non-CSV item: ${file}`);
            }
        }
    } catch (error) {
        console.error(`Error processing CSV files: ${error.message}`);
    }
}

// Function to check if a record already exists in Podio
async function checkForDuplicate(row, podioClient) {
    const url = `/item/app/${PODIO_APP_ID}/filter/`;
    console.log('Processing row', row.Address);

    const payload = {
        "filters": {
            "267139876": row.Address // Address Field ID
        },
        "limit": 30, // Optional, adjust as needed
        "offset": 0, // Optional, adjust for pagination if needed
        "remember": false // Optional, set to true if you want to remember the view
    };

    console.log(`Filtering by Name: "${row['Primary Name']}", Address: "${row.Address}"`);

    try {
        const response = await podioClient.request('POST', url, payload);
        console.log(`Response from Podio:`, response.filtered);

        // Remove duplicates and delete them from Podio
        let itemsToReturn;
        if (response?.items && response.items.length > 1) {
            itemsToReturn = response.items;
            console.log('Remaining unique items:', itemsToReturn);
        } else {
            itemsToReturn = response.items; // If there's only one item, return it directly
        }

        return itemsToReturn && itemsToReturn.length > 0;
    } catch (error) {
        console.error(`Error checking for duplicate:`, error.message, error.stack);
        return false;
    }
}

// Function to create a new Podio record
async function createPodioRecord(rowData, podioClient) {
    const url = `/item/app/${PODIO_APP_ID}/`;

    // Construct the payload
    const payload = {
        "fields": {
            "267139876": rowData.Address,
            "267139891": rowData.City,
            "267139892": rowData.State,
            "267570543": rowData.ZIP,// zip old
            "267139894": rowData.Type,
            "267139895": parseInt(rowData.Beds, 10),
            "267139896": parseFloat(rowData.Baths),
            "267139897": parseInt(rowData['Sq Ft'], 10),
            "267139898": parseInt(rowData['Yr Built'], 10),
            "267139899": rowData['Primary Name'],
            "267139900": rowData['Secondary Name'] || 'N/A',
            "267139901": [{ "type": "mobile", "value": rowData['Primary Phone1'] || '' }],
            "267139902": [{ "type": "mobile", "value": rowData['Primary Mobile Phone1'] || '' }],
            "267139903": [{ "type": "mobile", "value": rowData['Secondary Phone1'] || '' }],
            "267139904": [{ "type": "mobile", "value": rowData['Secondary Mobile Phone1'] || '' }],
            "267139910": [{ "type": "other", "value": rowData['Primary Email1'] || '' }],
            "267139911": [{ "type": "other", "value": rowData['Secondary Email1'] || '' }],
            "267139912": parseFloat(rowData['Est Value']) || 0,
            "267139913": parseFloat(rowData['Est Open Loans $']) || 0,
            "267139914": parseFloat(rowData['Purchase Amt']) || 0,
            "267139915": rowData['FCL Stage'] === 'Auction' ? 1 : 0,
            // Updated date format to include time
            "267139916": {
                "start": "2024-10-09 15:00:00",  // Updated to include time
                "end": "2024-10-09 15:00:00"     // Updated to include time
            },
            "267139917": {
                "start": "2024-08-27 15:00:00",  // Updated to include time
                "end": "2024-08-27 15:00:00"     // Updated to include time
            },
            "267139918": rowData['Sale Place'],
            "267139919": parseInt(rowData['TS Number']) || 0,
            "267140023": 1,
            "267140224": parseFloat(rowData['Default Amt']) || 0,
            "267140225": parseFloat(rowData['Est Open Loans $']) || 0,
            "267568787": 2, // skiptraced yes || no
        }
    };


    // Log the payload before sending

    try {
        const response = await podioClient.request('POST', url, payload);
        console.log(`Record created:`, response.fields);
    } catch (error) {
        console.error(`Error creating record:`, error.message);
        console.error(`Error details:`, error);
    }
}

// Example usage: authenticate first, then process CSV
const csvDirectoryPath = './paste_csv_here'; // Specify the directory path

function combineAllDuplicates(items) {
    // Use reduce to gather all duplicates into one array
    return items.reduce((acc, item) => {
        // Concatenate the current item's duplicates into the accumulator
        return acc.concat(item.duplicates);
    }, []); // Start with an empty array
}


async function deleteDuplicates(manicuredDuplicatesArray, podioClient) {
    for (const item of manicuredDuplicatesArray) {
        const itemId = item.item_id;

        try {
            console.log(`Deleting item with ID: ${itemId}`);
            // Send DELETE request to Podio API
            await podioClient.request('DELETE', `/item/${itemId}`);
            console.log(`Item with ID: ${itemId} deleted successfully`);
        } catch (error) {
            console.error(`Failed to delete item with ID: ${itemId}`, error.message);
        }
    }
}




// Function to get all items from the Podio app and check for duplicates
async function getDuplicates(podioClient) {
    const url = `/item/app/${PODIO_APP_ID}/filter/`;
    const payload = {
        "limit": 100, // Adjust limit based on your needs
        "offset": 0 // Pagination offset, start from 0
    };

    try {
        let allItems = [];
        let hasMoreItems = true;
        let offset = 0;

        // Paginate through all items in the app
        while (hasMoreItems) {
            payload.offset = offset;
            const response = await podioClient.request('POST', url, payload);
            if (response && response.items && response.items.length > 0) {
                allItems = allItems.concat(response.items);
                offset += response.items.length;
            } else {
                hasMoreItems = false;
            }
        }

        console.log(`Total items retrieved: ${allItems.length}`);

        // Group items by Address or any other key
        const groupedItems = allItems.reduce((acc, item) => {
            const addressFieldId = 267139876; // Replace with your actual address field ID
            const address = item.fields.find(f => f.field_id === addressFieldId)?.values?.[0]?.value || "unknown";

            if (!acc[address]) {
                acc[address] = [];
            }
            acc[address].push(item);

            return acc;
        }, {});

        const duplicates = [];

        // Determine duplicates within each group based on created_on and number of revisions
        Object.keys(groupedItems).forEach(address => {
            const items = groupedItems[address];

            if (items.length > 1) {
                // Sort items by created_on and revision count
                items.sort((a, b) => {
                    const dateA = new Date(a.created_on);
                    const dateB = new Date(b.created_on);

                    // If created_on is the same, check by revision count
                    if (dateA.getTime() === dateB.getTime()) {
                        return b.revision - a.revision;
                    }

                    return dateA - dateB;
                });

                // Keep the first item as the original and mark the rest as duplicates
                const original = items[0];
                const duplicateItems = items.slice(1);

                duplicates.push({
                    original,
                    duplicates: duplicateItems
                });
            }
        });

        console.log(`Total duplicates found: ${duplicates.length}`);

        return duplicates;

    } catch (error) {
        console.error(`Error retrieving items or checking for duplicates:`, error.message, error.stack);
        return [];
    }
}





// main functions to run 


async function findAndDeleteDuplicates() {
    try {
        await podio.authenticateWithApp(PODIO_APP_ID, PODIO_API_TOKEN, (err) => {

            if (err) throw new Error(err);

            podio.isAuthenticated().then(async () => {
                console.log('podio is authenticated in findAndDeleteDuplicates');

                // get array of duplicates
                let duplicates = await getDuplicates(podio);
                // console.log('duplicates in findAndDeleteDuplicates', duplicates);
                let manicuredDuplicatesArray = combineAllDuplicates(duplicates);
                await deleteDuplicates(manicuredDuplicatesArray, podio)


                // delete duplicates
            }).catch(err => console.log('something went wrong in authenticated podio of findAndDeleteDuplicates err', err));
        });
    } catch (error) {
        console.error('Error authenticating with Podio:', error);
        throw error;
    }
}

async function inputRecords() {


    try {
        await podio.authenticateWithApp(PODIO_APP_ID, PODIO_API_TOKEN, (err) => {

            if (err) throw new Error(err);

            let authenticated_podio = podio.isAuthenticated().then(() => {
                // Ready to make API calls in here...
                console.log('we are authenticated here run stuff ', podio);

                processCSV(csvDirectoryPath, podio);


                // return podio;
            }).catch(err => console.log(err));
            return authenticated_podio;
        });
    } catch (error) {
        console.error('Error authenticating with Podio:', error);
        throw error;
    }
}

async function main() {
    try {
       await inputRecords();
    // await findAndDeleteDuplicates();
    } catch (error) {
        console.error('Error during the process:', error);
    }
}

main();
