#!/usr/bin/python
from beem import Hive, Steem
from beem.account import Account
from beem.amount import Amount
from beem.block import Block
from beem.nodelist import NodeList
import pandas as pd

def init():
    data = {'Date': [],
            'Sent Amount' : [],
            'Sent Currency':[],
            'Received Amount':[],
            'Received Currency':[],
            'Fee Amount': [],
            'Fee Currency': [],
            'Net Worth Amount': [],
            'Net Worth Currency':[],
            'Label': [],
            'Description': [],
            'TxHash': []}  
    return data

def add_trade(data, timestamp, amount_in, amount_out, clarification="", description=""):

    data["Date"].append(timestamp)
    data["Description"].append(description)
    data["Fee Amount"].append("")
    data["Fee Currency"].append("")
    data["Net Worth Amount"].append("")
    data["Net Worth Currency"].append("")
    data["Label"].append(clarification)               
    data["Received Currency"].append(amount_in.symbol)
    data["Received Amount"].append(float(amount_in))
    data["TxHash"].append(trx_id)
    data["Sent Amount"].append(float(amount_out))
    data["Sent Currency"].append(amount_out.symbol)
    return data

def add_deposit(data, timestamp, amount, clarification="", description=""):
    data["Date"].append(timestamp)
    data["Description"].append(description)
    data["Fee Amount"].append("")
    data["Fee Currency"].append("")
    data["Net Worth Amount"].append("")
    data["Net Worth Currency"].append("")    
    data["Label"].append(clarification)               
    data["Received Currency"].append(amount.symbol)
    data["Received Amount"].append(float(amount))
    data["TxHash"].append(trx_id)
    data["Sent Amount"].append("")
    data["Sent Currency"].append("")
    return data

def add_withdrawal(data, timestamp, amount, clarification="", description=""):
    data["Date"].append(timestamp)
    data["Description"].append(description)
    data["Fee Amount"].append("")
    data["Fee Currency"].append("")
    data["Net Worth Amount"].append("")
    data["Net Worth Currency"].append("")    
    data["Label"].append(clarification)               
    data["Received Currency"].append("")
    data["Received Amount"].append("")
    data["TxHash"].append(trx_id)
    data["Sent Amount"].append(float(amount))
    data["Sent Currency"].append(amount.symbol)
    return data

def store(filename, data, sheet_name='Sheet1'):

    df = pd.DataFrame(data)
    
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df.to_excel(writer, sheet_name=sheet_name, startrow=0, header=True, index=False)   
    writer.save()

def store_csv(filename, data):

    df = pd.DataFrame(data)
    df.to_csv(filename, header=True, index=False)       


if __name__ == "__main__":
    nodelist = NodeList()
    nodelist.update_nodes()
    stm = Hive(node=nodelist.get_hive_nodes())
    # stm = Steem(node=nodelist.get_steem_nodes())
    print(stm)
        
    account_name = "holger80"
    data_account_name = "hive_%s_powered_up" % account_name
    symbol = "HIVE"
    hive_fork_block = 41818753
    has_fork = True
    limit_to_year = False
    current_year = 2020
    
    csv_filename = "%s_%d.csv" % (data_account_name, current_year)
    account = Account(account_name, blockchain_instance=stm)
    ops_dict = {}
    _ids = {}
    for ops in account.history():
        ops_dict[ops["index"]] = ops
        if ops["_id"] in _ids:
            _ids[ops["_id"]] += 1
        else:
            _ids[ops["_id"]] = 1
    duplicate_indices = []
    _id_list = []
    for _id in sorted(list(ops_dict.keys())):
        ops = ops_dict[_id]
        if _ids[ops["_id"]] == 1:
            continue
        if ops["_id"] not in _id_list:
            _id_list.append(ops["_id"])
        else:
            trx_id = ops["trx_id"]
            if trx_id == "0000000000000000000000000000000000000000":
                duplicate_indices.append(ops["index"])
            else:
                block = Block(ops["block"], blockchain_instance=stm)
                count_ops = 0
                for t in block.transactions:
                    if t["transaction_id"] != trx_id:
                        continue
                    for o in t["operations"]:
                        count_ops += 1
                if count_ops < _ids[ops["_id"]]:
                    duplicate_indices.append(ops["index"])

    type_count = {}
    for _id in sorted(list(ops_dict.keys())):
        ops = ops_dict[_id]
        if ops["type"] in type_count:
            type_count[ops["type"]] += 1
        else:
            type_count[ops["type"]] = 1
    
    symbol_amount = 0
    backed_symbol_amount = 0    
    index = 0
    hard_fork_reached = False
    year_reached = False
    next_year_reached = False
    data = init()
  

    print("duplicate indices %d" % len(duplicate_indices))

    for _id in sorted(list(ops_dict.keys())):
        ops = ops_dict[_id]
        if _id in duplicate_indices:
            continue
        block = ops["block"]
        timestamp = ops["timestamp"].replace("T", " ")
        trx_id = ops["trx_id"]
        if trx_id == "0000000000000000000000000000000000000000":
            trx_id = "virtual_id_" + ops["_id"]

        if  limit_to_year and not year_reached and timestamp[:4] == str(current_year):
            if has_fork and hard_fork_reached:
                year_reached = True
            elif has_fork:
                year_reached = False
            else:
                year_reached = True            
            if year_reached and symbol_amount > 0:
                amount = Amount(symbol_amount, symbol, blockchain_instance=stm)
                data = add_deposit(data, "%d-01-01 00:00:00" % current_year, amount,
                                   description="Virtual transfer to %d" % current_year)
  
        if limit_to_year and not next_year_reached and timestamp[:4] == str(current_year + 1):
            year_reached = True
            next_year_reached = False
            if symbol_amount > 0:
                amount = Amount(symbol_amount, symbol, blockchain_instance=stm)
                data = add_withdrawal(data, "%d-01-01 00:00:00" % (current_year + 1), amount,
                                      description="Virtual transfer to %d" % (current_year + 1))
                
        elif limit_to_year and next_year_reached:
            continue

        if has_fork and block > hive_fork_block and not hard_fork_reached:
            amount = Amount(symbol_amount, symbol, blockchain_instance=stm)
            data = add_deposit(data, timestamp, amount, description="Hard fork", clarification="fork")
            hard_fork_reached = True
            if  limit_to_year and not year_reached and timestamp[:4] == str(current_year):
                year_reached = True

        if ops["type"] == "transfer_to_vesting":
            amount = Amount(ops["amount"], blockchain_instance=stm)
            if ops["to"] == account_name:
                symbol_amount += float(amount)
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                data = add_deposit(data, timestamp, amount, description="Power up")

        elif ops["type"] == "fill_vesting_withdraw":
            amount = Amount(ops["deposited"], blockchain_instance=stm)
            if ops["from_account"] == account_name:
                symbol_amount -= float(amount)
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                if symbol_amount < 0:
                    data = add_deposit(data, timestamp, Amount(-round(symbol_amount, 3), symbol, blockchain_instance=stm),
                                       description="Staking reward", clarification="staking")
                    symbol_amount += (-round(symbol_amount, 3))
                data = add_withdrawal(data, timestamp, amount, description="Powering down")
            
    print("%d entries" % index)
    print("%s - %.3f %s" % (timestamp, symbol_amount, symbol))
    store_csv(csv_filename, data)