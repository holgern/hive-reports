#!/usr/bin/python
from beem import Hive, Steem
from beem.account import Account
from beem.amount import Amount
from beem.block import Block
from beem.nodelist import NodeList
from beem.utils import formatTimeString
from datetime import datetime
import pandas as pd
from collections import OrderedDict


def init():
    data = OrderedDict({'Type': [],
                        'Buy Amount' : [],
                        'Buy Cur.':[],
                        'Sell Amount':[],
                        'Sell Cur.':[],
                        'Fee Amount': [],
                        'Fee Cur.': [],
                        'Exchange': [],
                        'Trade Group':[],
                        'Comment': [],
                        'Date': [],
                        "Tx-ID": []})
    return data

def add_trade(data, timestamp, amount_in, amount_out, clarification="Trade", comment=""):
    data["Exchange"].append(exchange)
    data["Trade Group"].append(data_account_name)
    data["Date"].append(timestamp)
    data["Comment"].append(comment)
    data["Fee Amount"].append("")
    data["Fee Cur."].append("")
    data["Type"].append(clarification)               
    data["Buy Cur."].append(amount_in.symbol)
    data["Buy Amount"].append(round(float(amount_in), 3))
    data["Sell Amount"].append(round(float(amount_out), 3))
    data["Sell Cur."].append(amount_out.symbol)
    data["Tx-ID"].append(trx_id)
    return data

def add_deposit(data, timestamp, amount, clarification="Deposit", comment=""):
    data["Exchange"].append(exchange)
    data["Trade Group"].append(data_account_name)
    data["Date"].append(timestamp)
    data["Comment"].append(comment)
    data["Fee Amount"].append("")
    data["Fee Cur."].append("")
    data["Type"].append(clarification)               
    data["Buy Cur."].append(amount.symbol)
    data["Buy Amount"].append(round(float(amount), 3))
    data["Sell Amount"].append("")
    data["Sell Cur."].append("")
    data["Tx-ID"].append(trx_id)
    return data

def add_withdrawal(data, timestamp, amount, clarification="Withdrawal", comment=""):
    data["Exchange"].append(exchange)
    data["Trade Group"].append(data_account_name)
    data["Date"].append(timestamp)
    data["Comment"].append(comment)
    data["Fee Amount"].append("")
    data["Fee Cur."].append("")
    data["Type"].append(clarification)               
    data["Buy Cur."].append("")
    data["Buy Amount"].append("")
    data["Sell Amount"].append(round(float(amount), 3))
    data["Sell Cur."].append(amount.symbol)
    data["Tx-ID"].append(trx_id)
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
    # stm = Steem(node=nodelist.get_steem_nodes())
    stm = Hive(node=nodelist.get_hive_nodes())
    print(stm)
        
    account_name = "holger80"

    exchange = "hive"
    data_account_name = "%s" % account_name
    symbol = "HIVE"
    backed_symbol = "HBD"
    hive_fork_block = 41818753
    has_fork = True
    limit_to_year = True
    current_year = 2020
    date_format = "%Y-%m-%d %H:%M:%S"
    
    xls_filename = "%s_%s_%d.xlsx" % (exchange, data_account_name, current_year)
    csv_filename = "%s_%s_%d.csv" % (exchange, data_account_name, current_year)
    
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
        date = formatTimeString(ops["timestamp"]).strftime(date_format)
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
            if year_reached:
                if symbol_amount > 0:
                    amount = Amount(symbol_amount, symbol, blockchain_instance=stm)
                    data = add_deposit(data, datetime(current_year, 1, 1, 0, 0, 0).strftime(date_format), amount,
                                       comment="Virtual transfer to %d" % current_year)
                if backed_symbol_amount > 0:
                    amount = Amount(backed_symbol_amount, backed_symbol, blockchain_instance=stm)
                    data = add_deposit(data, datetime(current_year, 1, 1, 0, 0, 0).strftime(date_format), amount,
                                       comment="Virtual transfer to %d" % current_year)    
        if limit_to_year and not next_year_reached and timestamp[:4] == str(current_year + 1):
            year_reached = True
            next_year_reached = False
            if symbol_amount > 0:
                amount = Amount(symbol_amount, symbol, blockchain_instance=stm)
                data = add_withdrawal(data, datetime(current_year + 1, 1, 1, 0, 0, 0).strftime(date_format), amount,
                                       comment="Virtual transfer to %d" % (current_year + 1))
            if backed_symbol_amount > 0:
                amount = Amount(backed_symbol_amount, backed_symbol, blockchain_instance=stm)
                data = add_withdrawal(data, datetime(current_year + 1, 1, 1, 0, 0, 0).strftime(date_format), amount,
                                      comment="Virtual transfer to %d" % (current_year + 1))
                
        elif limit_to_year and next_year_reached:
            continue

        if has_fork and block > hive_fork_block and not hard_fork_reached:
            amount = Amount(symbol_amount, symbol, blockchain_instance=stm)
            data = add_deposit(data, date, amount, comment="Hard fork", clarification="Airdrop")
            amount = Amount(backed_symbol_amount, backed_symbol, blockchain_instance=stm)
            data = add_deposit(data, date, amount, comment="Hard fork", clarification="Airdrop")
            hard_fork_reached = True
            if  limit_to_year and not year_reached and timestamp[:4] == str(current_year):
                year_reached = True            
        
        if ops["type"] == "fill_convert_request":
            amount_out = Amount(ops["amount_out"], blockchain_instance=stm)
            amount_in = Amount(ops["amount_in"], blockchain_instance=stm)
            symbol_amount += float(amount_out)
            backed_symbol_amount -= float(amount_in)
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue
            data = add_trade(data, date, amount_out, amount_in, comment="Convert")
            
        elif ops["type"] == "transfer_from_savings":
            amount = Amount(ops["amount"], blockchain_instance=stm)
            if ops["to"] == account_name:
                if amount.symbol == symbol:
                    symbol_amount += float(amount)
                else:
                    backed_symbol_amount += float(amount)
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                data = add_deposit(data, date, amount, comment="Transfer from savings")
        elif ops["type"] == "transfer_to_savings":
            amount = Amount(ops["amount"], blockchain_instance=stm)
            if amount.symbol == symbol:
                symbol_amount -= float(amount)
            else:
                backed_symbol_amount -= float(amount)  
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue            
            data = add_withdrawal(data, date, amount, comment="Transfer to savings")
        elif ops["type"] == "transfer_to_vesting":
            amount = Amount(ops["amount"], blockchain_instance=stm)          
            if ops["from"] == account_name:
                symbol_amount -= float(amount)
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                data = add_withdrawal(data, date, amount, comment="Transfer to vesting")
        elif ops["type"] == "fill_vesting_withdraw":
            amount = Amount(ops["deposited"], blockchain_instance=stm)
            if ops["to_account"] == account_name:
                symbol_amount += float(amount)
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                data = add_deposit(data, date, amount, comment="Fill vesting withdraw")
        elif ops["type"] == "proposal_pay":
            amount = Amount(ops["payment"], blockchain_instance=stm)
            backed_symbol_amount += float(amount)
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue
            data = add_deposit(data, date, amount, comment="Proposal pay", clarification="Income")
        elif ops["type"] == "create_proposal":
            backed_symbol_amount -= 10
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue            
            data = add_withdrawal(data, date, Amount(10, backed_symbol, blockchain_instance=stm),
                                  comment="Create proposal", clarification="Spend")
        elif ops["type"] == "transfer":
            amount = Amount(ops["amount"], blockchain_instance=stm)
            if ops["from"] == account_name and ops["to"] == account_name:
                continue
            if ops["to"] == account_name:
                if amount.symbol == symbol:
                    symbol_amount += float(amount)
                else:
                    backed_symbol_amount += float(amount)
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                clarification = "Deposit"
                data = add_deposit(data, date, amount, clarification=clarification,
                                   comment="Transfer from %s" % ops["from"])
            else:
                if amount.symbol == symbol:
                    symbol_amount -= float(amount)
                else:
                    backed_symbol_amount -= float(amount)                
                index += 1
                if has_fork and block < hive_fork_block:
                    continue
                if limit_to_year and not year_reached:
                    continue
                clarification = "Withdrawal"
                data = add_withdrawal(data, date, amount, clarification=clarification,
                                      comment="Transfer to %s" % ops["to"])
    
        elif ops["type"] == "account_create_with_delegation":
            if ops["new_account_name"] == account_name:
                continue            
            fee = Amount(ops["fee"], blockchain_instance=stm)
            symbol_amount -= float(fee)
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue
            if float(fee) > 0:
                data = add_withdrawal(data, date, fee,clarification="Spend",
                                      comment="Account creation fee")
        elif ops["type"] == "account_create":
            if ops["new_account_name"] == account_name:
                continue
            fee = Amount(ops["fee"], blockchain_instance=stm)
            symbol_amount -= float(fee) 
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue
            if float(fee) > 0:
                data = add_withdrawal(data, date, fee, clarification="Spend", comment="Account creation fee")
        elif ops["type"] == "claim_reward_balance":
         
            reward_steem = Amount(ops["reward_steem"], blockchain_instance=stm)
            reward_vests = Amount(ops["reward_vests"], blockchain_instance=stm)
            reward_sbd = Amount(ops["reward_sbd"], blockchain_instance=stm)
            if float(reward_steem) > 0:
                symbol_amount += float(reward_steem)
            if float(reward_sbd) > 0:
                backed_symbol_amount += float(reward_sbd)
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue
            if float(reward_steem) > 0:
                data = add_deposit(data, date, reward_steem, clarification="Staking", comment="Claimed rewards")
            if float(reward_sbd) > 0:
                data = add_deposit(data, date, reward_sbd, clarification="Staking", comment="Claimed rewards")
                
          
        elif ops["type"] == "fill_order":
            open_pays = Amount(ops["open_pays"], blockchain_instance=stm)
            current_pays = Amount(ops["current_pays"], blockchain_instance=stm)
            open_owner = ops["open_owner"]
            current_owner = ops["current_owner"]
         
            if current_owner == account_name:
                if open_pays.symbol == symbol:
                    symbol_amount += float(open_pays)
                    backed_symbol_amount -= float(current_pays)
                else:
                    backed_symbol_amount += float(open_pays)
                    symbol_amount -= float(current_pays)
            else:
                if current_pays.symbol == symbol:
                    symbol_amount += float(current_pays)
                    backed_symbol_amount -= float(open_pays)
                else:
                    backed_symbol_amount += float(current_pays)
                    symbol_amount -= float(open_pays)
         
            index += 1
            if has_fork and block < hive_fork_block:
                continue
            if limit_to_year and not year_reached:
                continue            
            if current_owner == account_name:
                data = add_trade(data, date, open_pays, current_pays, comment="Internal market")
            else:
                data = add_trade(data, date, current_pays, open_pays, comment="Internal market")
            
    print("%d entries" % index)
    print("%s - %.3f %s, %.3f %s" % (timestamp, symbol_amount, symbol, backed_symbol_amount, backed_symbol))
    # store(xls_filename, data)
    store_csv(csv_filename, data)