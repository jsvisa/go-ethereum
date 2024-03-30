#!/usr/bin/env python3

import os
import json
import requests
import logging
import argparse


logging.basicConfig(
    format="[%(asctime)s] - %(levelname)s - %(message)s", level=logging.INFO
)


def get_block_prestate(provider, block_number):
    headers = {"content-type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "id": 67,
        "params": [
            hex(block_number),
            {
                "timeout": "120s",
                "tracer": "prestateTracer",
                "tracerConfig": {"diffMode": False},
            },
        ],
    }
    response = requests.post(provider, json=data, headers=headers)
    return response.json()["result"]


def get_block(provider, block_number, full_txs=False):
    headers = {"content-type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block_number), full_txs],
        "id": 1,
    }
    response = requests.post(provider, json=data, headers=headers)
    return response.json()["result"]


def merge_prestate(prestate):
    allocs = dict()
    for tx in prestate:
        for address, items in tx["result"].items():
            items["nonce"] = items.get("nonce", 0)
            items["balance"] = items.get("balance", "0x0")
            # not exists, add it
            if address not in allocs:
                allocs[address] = items
                continue

            # exists, merge it
            if "code" in items and "code" not in allocs[address]:
                allocs[address]["code"] = items["code"]

            if "storage" in items:
                if "storage" not in allocs[address]:
                    allocs[address]["storage"] = items["storage"]
                else:
                    for k, v in items["storage"].items():
                        if k not in allocs[address]["storage"]:
                            allocs[address]["storage"][k] = v

    return allocs


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--provider",
        "-p",
        default=os.getenv("ETHEREUM_ARCHIVE_PROVIDER"),
        help="HTTPProvider URL",
    )
    parser.add_argument(
        "--block",
        type=int,
        default=19527299,
        help="Gas price, in wei",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="out",
        help="Output directory",
    )
    args = parser.parse_args()

    provider = args.provider
    block = args.block
    output = args.output

    os.makedirs(args.output, exist_ok=True)

    prestate = get_block_prestate(provider, block)
    allocs = merge_prestate(prestate)
    with open("{}/alloc.json".format(output), "w") as f:
        json.dump(allocs, f, indent=2)

    current_block = get_block(provider, block, True)
    parent_block = get_block(provider, block - 1, False)
    with open("{}/txs.json".format(args.output), "w") as f:
        json.dump(current_block["transactions"], f, indent=2)

    # Coinbase              common.UnprefixedAddress            `json:"currentCoinbase"  `
    # Difficulty            *math.HexOrDecimal256               `json:"currentDifficulty"`
    # Random                *math.HexOrDecimal256               `json:"currentRandom"`
    # ParentDifficulty      *math.HexOrDecimal256               `json:"parentDifficulty"`
    # ParentBaseFee         *math.HexOrDecimal256               `json:"parentBaseFee,omitempty"`
    # ParentGasUsed         math.HexOrDecimal64                 `json:"parentGasUsed,omitempty"`
    # ParentGasLimit        math.HexOrDecimal64                 `json:"parentGasLimit,omitempty"`
    # GasLimit              math.HexOrDecimal64                 `json:"currentGasLimit"`
    # Number                math.HexOrDecimal64                 `json:"currentNumber"`
    # Timestamp             math.HexOrDecimal64                 `json:"currentTimestamp"`
    # ParentTimestamp       math.HexOrDecimal64                 `json:"parentTimestamp,omitempty"`
    # BlockHashes           map[math.HexOrDecimal64]common.Hash `json:"blockHashes,omitempty"`
    # Ommers                []ommer                             `json:"ommers,omitempty"`
    # Withdrawals           []*types.Withdrawal                 `json:"withdrawals,omitempty"`
    # BaseFee               *math.HexOrDecimal256               `json:"currentBaseFee,omitempty"`
    # ParentUncleHash       common.Hash                         `json:"parentUncleHash"`
    # ExcessBlobGas         *math.HexOrDecimal64       `json:"currentExcessBlobGas,omitempty"`
    # ParentExcessBlobGas   *math.HexOrDecimal64       `json:"parentExcessBlobGas,omitempty"`
    # ParentBlobGasUsed     *math.HexOrDecimal64       `json:"parentBlobGasUsed,omitempty"`
    # ParentBeaconBlockRoot *common.Hash               `json:"parentBeaconBlockRoot"`

    env = {
        "currentCoinbase": current_block["miner"],
        "currentDifficulty": current_block["difficulty"],
        "currentRandom": current_block["nonce"],
        "parentDifficulty": parent_block["difficulty"],
        "parentBaseFee": parent_block.get("baseFee", "0x0"),
        "parentGasUsed": parent_block["gasUsed"],
        "parentGasLimit": parent_block["gasLimit"],
        "currentGasLimit": current_block["gasLimit"],
        "currentNumber": current_block["number"],
        "currentTimestamp": current_block["timestamp"],
        "parentTimestamp": parent_block["timestamp"],
        "blockHashes": {},
        "ommers": current_block["uncles"],
        "withdrawals": current_block.get("withdrawals", []),
        "currentBaseFee": current_block.get("baseFee", "0x0"),
        "parentUncleHash": parent_block["sha3Uncles"],
        "currentExcessBlobGas": current_block.get("excessGas", "0x0"),
        "parentExcessBlobGas": parent_block.get("excessGas", "0x0"),
        "parentBlobGasUsed": parent_block.get("blobGasUsed", "0x0"),
        "parentBeaconBlockRoot": current_block.get("parentBeaconBlockRoot"),
    }

    with open("{}/env.json".format(output), "w") as f:
        json.dump(env, f, indent=2)


if __name__ == "__main__":
    main()
