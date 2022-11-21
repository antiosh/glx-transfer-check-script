const fs = require('fs');
const axios = require('axios');
const json2csv = require('json2csv');
const dayjs = require('dayjs');
const interface = require('@splinterlands/hive-interface');
const hive = new interface.Hive({
  rpc_nodes: [
    'https://anyx.io',
    'https://api.hive.blog',
    'https://hived.splinterlands.com',
  ],
});

const api = axios.create({
  baseURL: 'https://validator.genesisleaguesports.com',
});

async function getValidatorTransaction(transactionId) {
  try {
    const result = await api.get(`/transaction/${transactionId}`);
    return result.data;
  } catch (err) {
    console.log('VALIDATOR ERROR:', err);
  }
}

const allUserTransfers = [];
const allGlsHiveEngineTransfers = [];

const userTransfersMap = {};

// https://hiveblocks.com/b/69802648
// 2022-11-19T16:00:00
const START_BLOCK = 69802648;

// https://hiveblocks.com/b/69812239
// 2022-11-20T00:00:00
const END_BLOCK = 69812239;

// EXAMPLE BLOCK THAT HAS A gls-plat-token_transfer
// const START_BLOCK = 69811436;

// EXAMPLE BLOCK THAT HAS A ssc-mainnet-hive GLX transfer
// const START_BLOCK = 69837566;

// JUST FOR TESTING
// const END_BLOCK = START_BLOCK + 1;

async function processBlocks() {
  let currentBlock = START_BLOCK;
  while (currentBlock <= END_BLOCK) {
    await processBlock(currentBlock);
    currentBlock++;
  }
}

async function processBlock(blockNumber) {
  const block = await hive.api('get_block', [blockNumber]);

  if (!block || !block.transactions) {
    console.log('Could not get block transactions for block:', blockNumber);
    return;
  }

  const blockTime = new Date(block.timestamp + 'Z');

  // Loop through all of the transactions and operations in the block
  for (let i = 0; i < block.transactions.length; i++) {
    const transaction = block.transactions[i];
    for (
      let operationIndex = 0;
      operationIndex < transaction.operations.length;
      operationIndex++
    ) {
      const operation = transaction.operations[operationIndex];
      await processOperation(
        operation,
        blockNumber,
        block.block_id,
        block.previous,
        block.transaction_ids[i],
        blockTime
      );
    }
  }
}

function mapUserTransferData(blockNum, blockId, transactionId, account, quantity) {
  return {
    blockNum,
    blockId,
    transactionId,
    account,
    quantity,
  };
}

async function processOperation(
  op,
  block_num,
  block_id,
  prev_block_id,
  transactionId,
  block_time
) {
  const opName = op[0];
  const opData = op[1];
  if (opName === 'custom_json') {
    const json = tryParse(opData.json);
    // USER TRANSFER
    if (json) {
      if (opData?.id === 'gls-plat-token_transfer') {
        if (json.to === 'gls-he' && json.token === 'GLX') {
          const account = json.memo || opData.required_auths[0] || opData.required_posting_auths[0];
          const amount = parseFloat(json.qty);
          const validatorTransaction = await getValidatorTransaction(transactionId);
          if (validatorTransaction.success) {
            const data = mapUserTransferData(block_num, block_id, transactionId, account, amount);
            if (userTransfersMap[`${account}-${amount}`]) {
              userTransfersMap[`${account}-${amount}`].push({
              ...data,
              handled: false,
              });
            } else {
              userTransfersMap[`${account}-${amount}`] = [{
                ...data,
                handled: false,
              }];
            }
            allUserTransfers.push(data);
            console.log(`Added ${account} ${amount} GLX`);
          }
        }
      }
      // GLS HE TRANSFER
      else if (
        opData?.id === 'ssc-mainnet-hive' &&
        opData.required_auths[0] === 'gls-he' &&
        json.contractAction === 'transfer'
      ) {
        // json.contractPayload.memo ???
        const account = json.contractPayload.to;
        const amount = json.contractPayload.quantity;
        allGlsHiveEngineTransfers.push(mapUserTransferData(block_num, block_id, transactionId, account, amount));
        if (userTransfersMap[`${account}-${amount}`]) {
          const transfers = userTransfersMap[`${account}-${amount}`];
          for (let i = 0; i < transfers.length; i++) {
            const transfer = userTransfersMap[i];
            if (!transfer.handled) {
              transfer.handled = true;
              console.log(`Handled ${account} ${amount} GLX`);
              break;
            }
          }
        }
      }
    }
  }
}

function tryParse(json) {
  try {
    return JSON.parse(json);
  }
 catch (err) {
    console.log(`Error trying to parse JSON: ${json}`);
    return null;
  }
}

function writeCsv(fileName, data) {
  if (!data || data.length === 0) {
    console.log('No data for:', fileName);
    return;
  }
  const csvData = json2csv.parse(data);
  fs.writeFile(fileName, csvData, function (err) {
    if (err) {
      console.log(`${fileName} error`);
      console.log(err);
    }
    if (!err) {
      console.log(`Saved ${fileName}!`);
    }
  });
}

processBlocks().then(() => {
  const dateString = dayjs().format('YYYY-MM-DD_HH_MM');
  const userTransfersFileName = `${dateString}_user_transfers.csv`;
  const glsHiveEngineTransfersFileName = `${dateString}_gls_hive_engine_transfers.csv`;
  const unhandledTransfersFileName = `${dateString}_unhandled_transfers.csv`;
  const unhandled = Object.values(userTransfersMap).flat().filter((t) => !t.handled);
  writeCsv(userTransfersFileName, allUserTransfers);
  writeCsv(glsHiveEngineTransfersFileName, allGlsHiveEngineTransfers);
  writeCsv(unhandledTransfersFileName, unhandled);
  console.log('FINISHED!');
}).catch((err) => {
  console.log('ERROR PROCESSING', err);
});