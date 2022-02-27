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

map<string, string> read_pairs(string file_name, vector<string>& PairOneSymbols, vector<string>& PairTwoSymbols) {

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

int CreatePairPricesTable(sqlite3* db)
{
	string sql_CreateTable = string("CREATE TABLE IF NOT EXISTS PairPrices ")
		+ "(symbol1 CHAR(20) NOT NULL, "
		+ "symbol2 CHAR(20) NOT NULL, "
		+ "date CHAR(20) NOT NULL, "
		+ "open1 REAL NOT NULL, "
		+ "close1 REAL NOT NULL, "
		+ "open2 REAL NOT NULL, "
		+ "close2 REAL NOT NULL, "
		+ "profit_loss REAL NOT NULL, "
		+ "PRIMARY KEY(symbol1, symbol2, date), "
		+ "FOREIGN KEY(symbol1, date) REFERENCES PairOnePrices(symbol, date) "
		+ "ON DELETE CASCADE ON UPDATE CASCADE,\n"
		+ "FOREIGN KEY(symbol2, date) REFERENCES PairTwoPrices(symbol, date) "
		+ "ON DELETE CASCADE ON UPDATE CASCADE,\n"
		+ "FOREIGN KEY(symbol1, symbol2) REFERENCES StockPairs(symbol1, symbol2) "
		+ "ON DELETE CASCADE ON UPDATE CASCADE"
		+ ");";
	if (ExecuteSQL(db, sql_CreateTable.c_str()) == -1)
		return -1;
	cout << "Successfully create PairPrices Table." << endl;

	return 0;
}

int PopulatePairPrices(sqlite3* db)
{
	string sql_insert = string("INSERT INTO PairPrices ")
		+ "SELECT StockPairs.symbol1 as symbol1, StockPairs.symbol2 as symbol2, "
		+ "PairOnePrices.date as date, "
		+ "PairOnePrices.open as open1, PairOnePrices.close as close1, "
		+ "PairTwoPrices.open as open2, PairTwoPrices.close as close2, "
		+ "0 as profit_loss "
		+ "FROM StockPairs, PairOnePrices, PairTwoPrices "
		+ "WHERE (((StockPairs.symbol1 = PairOnePrices.symbol) AND "
		+ "(StockPairs.symbol2 = PairTwoPrices.symbol)) AND "
		+ "(PairOnePrices.date = PairTwoPrices.date)) "
		+ "ORDER BY symbol1, symbol2;";
	if (ExecuteSQL(db, sql_insert.c_str()) == -1)
		return -1;
	cout << "Successfully populate PairPrices Table." << endl;

	return 0;
}

int CalculateVolatility(sqlite3* db, string back_test_start_date)
{
	string calculate_volatility_for_pair = string("Update StockPairs SET volatility =")
		+ "(SELECT(AVG((adjusted_close1/adjusted_close2)*(adjusted_close1/ adjusted_close2)) - AVG(adjusted_close1/adjusted_close2)*AVG(adjusted_close1/adjusted_close2)) as variance "
		+ "FROM (SELECT StockPairs.symbol1 as symbol1, StockPairs.symbol2 as symbol2, "
		+ "PairOnePrices.date as date, "
		+ "PairOnePrices.adjusted_close as adjusted_close1, "
		+ "PairTwoPrices.adjusted_close as adjusted_close2 "
		+ "FROM StockPairs, PairOnePrices, PairTwoPrices "
		+ "WHERE(((StockPairs.symbol1 = PairOnePrices.symbol) AND "
		+ "(StockPairs.symbol2 = PairTwoPrices.symbol)) AND "
		+ "(PairOnePrices.date = PairTwoPrices.date)) "
		+ "ORDER BY symbol1, symbol2) as adjClose "
		+ "WHERE StockPairs.symbol1 = adjClose.symbol1 AND StockPairs.symbol2 = adjClose.symbol2 AND adjClose.date <= \'"
		+ back_test_start_date + "\');";
	if (ExecuteSQL(db, calculate_volatility_for_pair.c_str()) == -1)
		return -1;
	cout << "Successfully calculate volatility for pairs." << endl;

	return 0;
}

int BackTest(sqlite3* db, float k)
{
	stringstream kk;
	kk << k;
	string kstr = kk.str();

	//string sql_DropTable_Trade = string("TRUNCATE TABLE Trade;");
	string sql_CreateTable_Trade = string("CREATE TABLE IF NOT EXISTS Trade ")
		+ "(symbol1 CHAR(20) NOT NULL, "
		+ "symbol2 CHAR(20) NOT NULL, "
		+ "date CHAR(20) NOT NULL, "
		+ "profit_loss REAL NOT NULL, "
		+ "PRIMARY KEY(symbol1, symbol2, date), "
		+ "FOREIGN KEY(symbol1, symbol2, date) REFERENCES PairPrices(symbol1, symbol2, date) "
		+ "ON DELETE CASCADE ON UPDATE CASCADE\n"
		+ ");";

	    string sql_Insert_Trade = string("insert into Trade ")
		+ "select a.symbol1, a.symbol2, a.date as `date`,"
		+ "case when abs(b.close1/b.close2 - a.open1/a.open2)>volatility*" + kstr + " "
		+ "then - (10000*(a.open1 - a.close1) - 10000*a.open1/a.open2*(a.open2 - a.close2)) "
		+ "when abs(b.close1/b.close2 - a.open1/a.open2)<volatility*" + kstr + " "
		+ "then (10000*(a.open1 - a.close1) - 10000*a.open1/a.open2*(a.open2 - a.close2)) "
		+ "else 0 end as profit "
		+ "from PairPrices a join PairPrices b on julianday(a.date) - julianday(b.date)=1 "
		+ "and a.date>='2022-01-01' and a.symbol1=b.symbol1 and a.symbol2=b.symbol2 "
		+ "left join StockPairs c on a.symbol1=c.symbol1 and a.symbol2=c.symbol2;";

		
	    string sql_Update_PairPrices = string("Update PairPrices set profit_loss = ")
		+ "(SELECT profit_loss as profit_loss FROM Trade "
		+ "WHERE PairPrices.symbol1 = Trade.symbol1 "
		+ "AND PairPrices.symbol2 = Trade.symbol2 AND PairPrices.date = Trade.date);";

	//	if (ExecuteSQL(db, sql_DropTable_Trade.c_str()) == -1) return -1;
		if (ExecuteSQL(db, sql_CreateTable_Trade.c_str()) == -1) return -1;
		cout << "Already created Trade table;" << endl;
		if (ExecuteSQL(db, sql_Insert_Trade.c_str()) == -1) return -1;
		cout << "Already insert values into Trade;" << endl;
		if (ExecuteSQL(db, sql_Update_PairPrices.c_str()) == -1) return -1;
		cout << "Already update profit_loss of PairPrices;" << endl;
		return 0;
}

int BackTest2(sqlite3* db, float k);

int Update_Profitloss(sqlite3* db)
{
	string sql_Update_StockPairs = string("Update StockPairs SET profit_loss = ")
		+ "(select sum(profit_loss) as profit_loss"
		+ "from PairPrices"
		+ "where StockPairs.symbol1=PairPrices.symbol1"
		+ "and StockPairs.symbol2=PairPrices.symbol2"
		+ "group by PairPrices.symbol1, PairPrices.symbol2);"
		;

	if (ExecuteSQL(db, sql_Update_StockPairs.c_str()) == -1) return -1;
	cout << "Already update profit_loss of StockPairs;" << endl;

	return 0;
}



int main() {

	sqlite3* db = NULL;
	string database_name = "PairTrading.db";
	string pairtxt = "PairTrading.txt";
	vector<string> PairOneSymbols, PairTwoSymbols;
	string back_test_start_date = "2021-12-31";
	char user_option;
	if (OpenDatabase(database_name.c_str(), db) != 0)	  return -1;
	float k;


	while (true) {
		cout << "-----------menu-------------" << endl;
		cout << "A - Create and Populate Pair Table" << endl;
		cout << "B - Retrieve adn Populate Historical Data for Each Stock" << endl;
		cout << "C - Create and Populate Pair Table" << endl;
		cout << "D - Calculate Volatility" << endl;
		cout << "E - Back Test" << endl;
		cout << "F - Calculate Profit and Loss for Each Pair" << endl;
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
		case 'C':
			if (CreatePairPricesTable(db) == -1) return -1;
			if (PopulatePairPrices(db) == -1) return -1;
			break;
		case 'D':
			if (CalculateVolatility(db, back_test_start_date) == -1) return -1;
			break;
		case 'E':
			cout << "Please enter K:" << endl;
			cin >> k;
			if (BackTest(db, k) == -1) return -1;
			break;
		case 'F':
			if (Update_Profitloss(db) == -1) return -1;
			break;
		case 'X':
			exit(0);
		}

	}
	return 0;
}







int BackTest2(sqlite3* db, float k)
{
	stringstream kk;
	kk << k;
	string kstr = kk.str();

	//string sql_DropTable_Trade = string("TRUNCATE TABLE Trade;");
	string sql_CreateTable_Trade = string("CREATE TABLE IF NOT EXISTS Trade ")
		+ "(symbol1 CHAR(20) NOT NULL, "
		+ "symbol2 CHAR(20) NOT NULL, "
		+ "date CHAR(20) NOT NULL, "
		+ "profit_loss REAL NOT NULL, "
		+ "PRIMARY KEY(symbol1, symbol2, date), "
		+ "FOREIGN KEY(symbol1, symbol2, date) REFERENCES PairPrices(symbol1, symbol2, date) "
		+ "ON DELETE CASCADE ON UPDATE CASCADE"
		+ ");";

	string sql_Insert_Trade = string("insert into Trade ")
		+ "select a.symbol1, a.symbol2, a.date as `date`,"
		+ "case when b.close1/b.close2 - a.open1/a.open2>volatility*" + kstr + " "
		+ "then - (10000*(a.open1 - a.close1) - 10000*a.open1/a.open2*(a.open2 - a.close2)) "
		+ "when b.close1/b.close2 - a.open1/a.open2<-volatility*" + kstr + " "
		+ "then (10000*(a.open1 - a.close1) - 10000*a.open1/a.open2*(a.open2 - a.close2)) "
		+ "else 0 end as profit "
		+ "from PairPrices a left join PairPrices b on julianday(a.date) - julianday(b.date)=1 "
		+ "and a.date>='2022-01-01' and a.symbol1=b.symbol1 and a.symbol2=b.symbol2 "
		+ "left join StockPairs c on a.symbol1=c.symbol1 and a.symbol2=c.symbol2;";


	string sql_Update_PairPrices = string("Update PairPrices set profit_loss = ")
		+ "(SELECT profit_loss as profit_loss FROM Trade "
		+ "WHERE PairPrices.symbol1 = Trade.symbol1 "
		+ "AND PairPrices.symbol2 = Trade.symbol2 AND PairPrices.date = Trade.date);";

	//	if (ExecuteSQL(db, sql_DropTable_Trade.c_str()) == -1) return -1;
	if (ExecuteSQL(db, sql_CreateTable_Trade.c_str()) == -1) return -1;
	cout << "Already created Trade table;" << endl;
	if (ExecuteSQL(db, sql_Insert_Trade.c_str()) == -1) return -1;
	cout << "Already insert values into Trade;" << endl;
	if (ExecuteSQL(db, sql_Update_PairPrices.c_str()) == -1) return -1;
	cout << "Already update profit_loss of PairPrices;" << endl;
	return 0;
}



/*
* insert into Trade
select a.symbol1, a.symbol2, a.date as `date`,
case when abs(b.close1/b.close2 - a.open1/a.open2)>volatility*1
then - (10000*(a.open1 - a.close1) - 10000*a.open1/a.open2*(a.open2 - a.close2))
when abs(b.close1/b.close2 - a.open1/a.open2)<volatility*1
then (10000*(a.open1 - a.close1) - 10000*a.open1/a.open2*(a.open2 - a.close2))
else 0 end as profit
from PairPrices a left join PairPrices b on julianday(a.date) - julianday(b.date)=1
and a.date>='2022-01-01' and a.symbol1=b.symbol1 and a.symbol2=b.symbol2
left join StockPairs c on a.symbol1=c.symbol1 and a.symbol2=c.symbol2
*/
