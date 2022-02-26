#include "Stock.h"
#include "MarketData.h"
#include "Database.h"
#include <map>
#include <curl/curl.h>
#include "Util.h"
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
using namespace std;
using namespace std;

map<string,string> read_pairs(string file_name, vector<string>& PairOneSymbols, vector<string>& PairTwoSymbols) {
	
	ifstream fin;
	fin.open(file_name, ios::in);
	string line, symbol1, symbol2;
	map<string, string> pairs;
	while (!fin.eof())
	{
		getline(fin, line);
		stringstream sin(line);
		getline(sin, symbol1, ',');
		getline(sin, symbol2);
		PairOneSymbols.push_back(symbol1);
		PairTwoSymbols.push_back(symbol2);
		pairs.insert({ symbol1,symbol2 });
	}

	return pairs;
}

int CreatePairTable(sqlite3* db) {
	string sql_CreateTable_StockPairs = string("CREATE TABLE IF NOT EXISTS StockPairs ")
		+ "(id INT NOT NULL,"
		+ "symbol1 CHAR(20) NOT NULL,"
		+ "symbol2 CHAR(20) NOT NULL,"
		+ "volatility REAL NOT NULL,"
		+ "profit_loss REAL NOT NULL,"
		+ "PRIMARY KEY(symbol1, symbol2)"
		+ ");";
	if (ExecuteSQL(db, sql_CreateTable_StockPairs.c_str()) == -1) return -1;

	string sql_CreateTable_PairOnePrices = string("CREATE TABLE IF NOT EXISTS PairOnePrices ")
		+ "(symbol CHAR(20) NOT NULL,"
		+ "date CHAR(20) NOT NULL,"
		+ "open REAL NOT NULL,"
		+ "high REAL NOT NULL,"
		+ "low REAL NOT NULL,"
		+ "close REAL NOT NULL,"
		+ "adjusted_close REAL NOT NULL,"
		+ "volume INT NOT NULL,"
		+ "PRIMARY KEY(symbol, date)"
		+ ");";
	if (ExecuteSQL(db, sql_CreateTable_PairOnePrices.c_str()) == -1) return -1;

	string sql_CreateTable_PairTwoPrices = string("CREATE TABLE IF NOT EXISTS PairTwoPrices ")
		+ "(symbol CHAR(20) NOT NULL,"
		+ "date CHAR(20) NOT NULL,"
		+ "open REAL NOT NULL,"
		+ "high REAL NOT NULL,"
		+ "low REAL NOT NULL,"
		+ "close REAL NOT NULL,"
		+ "adjusted_close REAL NOT NULL,"
		+ "volume INT NOT NULL,"
		+ "PRIMARY KEY(symbol, date)"
		+ ");";
	if (ExecuteSQL(db, sql_CreateTable_PairTwoPrices.c_str()) == -1) return -1;
	cout << "successfully create pair tables" << endl;
	return 0;
}

int PopulateStockPairs(sqlite3* db, string pairtxt, vector<string>& PairOneSymbols, vector<string>& PairTwoSymbols) {
	map<string, string> pairs = read_pairs(pairtxt, PairOneSymbols, PairTwoSymbols);
	int id = 1;
	for (map<string, string>::iterator itr = pairs.begin(); itr != pairs.end(); itr++) {
		char sql_Insert[512];
		string symbol1 = itr->first;
		string symbol2 = itr->second;

		sprintf(sql_Insert, "INSERT or REPLACE INTO StockPairs(id,symbol1,symbol2,volatility,profit_loss) VALUES(%d, \"%s\",\"%s\", %f, %f)", id++, symbol1.c_str(), symbol2.c_str(), 0.0, 0.0);
		if (ExecuteSQL(db, sql_Insert) == -1)
			return -1;

	}
	return 0;
}



int main() {

	sqlite3* db = NULL;
	string database_name = "PairTrading.db";
	string pairtxt = "PairTrading.txt";
	vector<string> PairOneSymbols, PairTwoSymbols;
	char user_option;
	if (OpenDatabase(database_name.c_str(), db) != 0)	  return -1;
	


	while (true) {
		cout << "-----------menu-------------" << endl;
		cout << "A - Create and Populate Pair Table" << endl;
		cout << "B - Retrieve adn Populate Historical Data for Each Stock" << endl;
		cout << "X - Exit" << endl << endl;;


		cout << "please enter a valid option:" << endl;
		cin >> user_option;
		switch (user_option) 
		{
		case 'A':
			if (CreatePairTable(db) != 0) return -1;
			if (PopulateStockPairs(db, pairtxt, PairOneSymbols, PairTwoSymbols)) return -1;
			break;
		case 'B':
			//retrieve data from eodhistorical for the first time or update data
			//the function would first retrieve the last date in the database and only update data between last_date_in_db and end_date
			if (PullPairDataToDB(PairOneSymbols, db, true) == -1) return -1;
			cout << "successfully populate pair one data" << endl;
			if (PullPairDataToDB(PairTwoSymbols, db, false) == -1) return -1;
			cout << "successfully populate pair two data" << endl;
			break;
		case 'X':
			exit(0);
		}
		
	}
	return 0;
}