#pragma once
#include <string>
#include <vector>
#include <iostream>
#include "Database.h"

class Stock;
int PullMarketData(const std::string& url_request, std::string& read_buffer);
int PopulateDailyTrades(const std::string& read_buffer,
	Stock& stock);
std::string get_latest_date(sqlite3* db);
int PullPairDataToDB(std::vector<std::string> symbols, sqlite3* db,bool isPairOne);