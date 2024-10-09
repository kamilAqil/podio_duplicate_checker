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
                        const { hasDuplicates, ids } = await checkForDuplicate(row, podioClient);
                        console.log('hasDuplicates in createStream ',hasDuplicates);
                        
                        if (!hasDuplicates) {
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

async function checkForDuplicate(row, podioClient) {

    // Determine which address to use
    const propertyAddress = row.Input_Property_Address || row.Address;

    // Check if propertyAddress is not empty
    if (!propertyAddress || propertyAddress.trim() === '') {
        return {
            hasDuplicates: false,
            ids: []
        };
    }

    const url = `/item/app/${PODIO_APP_ID}/filter/`;

    const payload = {
        "filters": {
            "267139876": propertyAddress // Use the chosen address Field ID
        },
        "limit": 30, // Optional, adjust as needed
        "offset": 0, // Optional, adjust for pagination if needed
        "remember": false // Optional, set to true if you want to remember the view
    };

    try {
        const response = await podioClient.request('POST', url, payload);
        console.log('podioClient response for checkForDuplicate ', response.filtered);

        // Initialize an array to hold the IDs of duplicate items
        let duplicateIds = [];

        if (response?.items && response.items.length > 0) {
            // If duplicates are found, extract their IDs
            duplicateIds = response.items.map(item => item.item_id);
        }

        // Return both the boolean indicating if duplicates exist and the array of IDs
        return {
            hasDuplicates: duplicateIds.length > 0,
            ids: duplicateIds
        };
    } catch (error) {
        console.error(`Error checking for duplicate:`, error.message, error.stack);
        return {
            hasDuplicates: false,
            ids: []
        }; // Return empty IDs array on error
    }
}


// Function to create a new Podio record
async function createPodioRecord(rowData, podioClient) {
    const url = `/item/app/${PODIO_APP_ID}/`;
    // Extract Orig Sale Date and Sale Time from rowData
    const origSaleDate = rowData['Orig Sale Date'];  // Format: YYYY-MM-DD
    const saleTime = rowData['Sale Time'];           // Format: HH:MM:SS
    const fclRecDate = rowData['FCL Rec Date'];      // Format: YYYY-MM-DD
    const formattedFclRecDateTime = `${fclRecDate} 15:00:00`;  // "2024-09-13 15:00:00"

    // Combine Orig Sale Date and Sale Time into the correct format
    const formattedSaleDateTime = `${origSaleDate} ${saleTime}`; // "2024-10-18 09:00:00"
    // Construct the payload
    const payload = {
        "fields": {
            "267139876": rowData.Address,
            "267139891": rowData.City,
            "267139892": rowData.State,
            "267570543": rowData.ZIP, // zip old
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
            "267139915": 1,
            // Use the formatted Orig Sale Date and Sale Time
            "267139916": {
                "start": formattedSaleDateTime,
                "end": formattedSaleDateTime
            },
            // FCL Rec Date, using the formatted date and default time
            "267139917": {
                "start": formattedFclRecDateTime,
                "end": formattedFclRecDateTime
            },
            "267139918": rowData['Sale Place'],
            "267139919": parseInt(rowData['TS Number']) || 0,
            "267140023": 1,
            "267140224": parseFloat(rowData['Default Amt']) || 0,
            "267140225": parseFloat(rowData['Est Open Loans $']) || 0,
            "267568787": 2 // skiptraced yes || no
        }
    };


    // Log the payload before sending

    try {
        const response = await podioClient.request('POST', url, payload);
        console.log(`Record created:`, response.link);
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


async function processSkippedCsv(directoryPath, podioClient) {
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
                        try {
                            // Check for duplicates using the specified field IDs (e.g., address or unique identifier)
                            const { hasDuplicates, ids } = await checkForDuplicate(row, podioClient);
                            console.log('hasDuplicates', hasDuplicates);

                            if (hasDuplicates) {

                                
                                let objForUpdate = {
                                    "fields": { // Wrap your fields inside the "fields" object
                                        "267139901": [{ "type": "home", "value": row['Phone1_Number'] || '' }],  // Changed "mobile" to "home"
                                        "267139902": [{ "type": "home", "value": row['Phone2_Number'] || '' }],
                                        "267139903": [{ "type": "home", "value": row['Phone3_Number'] || '' }],
                                        "267139904": [{ "type": "home", "value": row['Phone4_Number'] || '' }],
                                        "267139905": [{ "type": "home", "value": row['Phone5_Number'] || '' }],
                                        "267139906": [{ "type": "home", "value": row['Phone6_Number'] || '' }],
                                        "267139907": [{ "type": "home", "value": row['Phone7_Number'] || '' }],
                                        "267139908": [{ "type": "home", "value": row['Phone8_Number'] || '' }],
                                        "267139909": [{ "type": "home", "value": row['Phone9_Number'] || '' }],
                                        "267139910": [{ "type": "home", "value": row['Phone10_Number'] || '' }],
                                        "267568787": 1
                                    }
                                };

                                // Loop through each duplicate ID and update them
                                for (const id of ids) {
                                    try {
                                        const updateUrl = `/item/${id}`; // Correct Podio API URL for updating an item
                                      
                                        const response = await podioClient.request('PUT', updateUrl, objForUpdate); // Capture the response from the API

                                        // Check if the response indicates success (you can adjust based on the actual response structure)
                                        if (response) {
                                            console.log(`Successfully updated item with ID: ${id}, Response:`, response);                                            
                                        } else {
                                            console.log(`Failed to update item with ID: ${id}, Response:`, response);
                                        }
                                    } catch (updateError) {
                                        console.error(`Error updating item with ID: ${id}, Error: ${updateError}`,updateError);
                                    }
                                }


                                
                            } else {
                                console.log(`No duplicate found for row: ${row.Address}, skipping.`);
                            }
                        } catch (error) {
                            console.error(`Error processing row: ${row.Address}, Error: ${error.message}`);
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

async function updateSkippedRecordsInPodio() {
    try {
        await podio.authenticateWithApp(PODIO_APP_ID, PODIO_API_TOKEN, (err) => {

            if (err) throw new Error(err);

            let authenticated_podio = podio.isAuthenticated().then(async() => {
                // Ready to make API calls in here...
                console.log('we are authenticated in updateSkippedRecordsInPodio here run stuff ', podio);
                let csvDirectoryPath = './paste_skipped_csv_here';
                await processSkippedCsv(csvDirectoryPath,podio);



                // return podio;
            }).catch(err => console.log(err));
            return authenticated_podio;
        });
    } catch (error) {
        console.error('Error authenticating with Podio:', error);
        throw error;
    }
}

async function findAndDeleteDuplicates() {
    try {
        await podio.authenticateWithApp(PODIO_APP_ID, PODIO_API_TOKEN, (err) => {

            if (err) throw new Error(err);

            podio.isAuthenticated().then(async () => {
                console.log('podio is authenticated in findAndDeleteDuplicates');

                // get array of duplicates
                let duplicates = await getDuplicates(podio);
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
                console.log('we are authenticated here run stuff ');

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
    //    await inputRecords();
    // await findAndDeleteDuplicates();
        await updateSkippedRecordsInPodio();
    } catch (error) {
        console.error('Error during the process:', error);
    }
}

main();
