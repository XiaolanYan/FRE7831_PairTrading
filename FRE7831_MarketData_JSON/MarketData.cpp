#include "MarketData.h"
#include <stdio.h>

#include "json/json.h"
#include "curl/curl.h"

#include "Stock.h"
#include "Util.h"
#include "Database.h"


int PullMarketData(const std::string& url_request, std::string& read_buffer) {
	//global initiliation of curl before calling a function

	//creating session handle
	CURL* handle;

	// Store the result of CURL’s webpage retrieval, for simple error checking.
	CURLcode result;

	handle = curl_easy_init();

	if (!handle)
	{
		cout << "curl_easy_init failed" << endl;
		return -1;
	}


	curl_easy_setopt(handle, CURLOPT_URL, url_request.c_str());

	//adding a user agent
	curl_easy_setopt(handle, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0");
	curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0);
	curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0);

	// send all data to this function 
	curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, WriteCallback);

	// we pass our 'chunk' struct to the callback function 
	curl_easy_setopt(handle, CURLOPT_WRITEDATA, &read_buffer);

	//perform a blocking file transfer
	result = curl_easy_perform(handle);

	// check for errors 
	if (result != CURLE_OK) {
		fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(result));
	}
	curl_easy_cleanup(handle);

	return 0;
}


int PopulateDailyTrades(const std::string& read_buffer,
	Stock& stock)
{
	Json::CharReaderBuilder builder;
	Json::CharReader* reader = builder.newCharReader();
	Json::Value root;   // will contains the root value after parsing.
	string errors;


	bool parsingSuccessful = reader->parse(read_buffer.c_str(), read_buffer.c_str() + read_buffer.size(), &root, &errors);
	if (not parsingSuccessful)
	{
		// Report failures and their locations in the document.
		cout << "Failed to parse JSON" << endl << read_buffer << errors << endl;
		return -1;
	}
	else
	{
		//cout << "\nSucess parsing json\n" << root << endl;
		float open, high, low, close, adjusted_close;// adjusted_close;
		long volume;
		string date;
		char time_tmp[30];
		for (Json::Value::const_iterator itr = root.begin(); itr != root.end(); itr++)
		{
			date = (*itr)["date"].asString();

			open = (*itr)["open"].asFloat();
			high = (*itr)["high"].asFloat();
			low = (*itr)["low"].asFloat();
			close = (*itr)["close"].asFloat();
			adjusted_close = (*itr)["adjusted_close"].asFloat();

			//adjusted_close = (*itr)["adjusted_close"].asFloat();
			volume = (*itr)["volume"].asInt64();
			DailyTrade aTrade(date, open, high, low, close, adjusted_close, volume);;
			stock.addDailyTrade(aTrade);

		}
		return 0;
	}
}

string get_latest_date(sqlite3* db) {
	int rc = 0;
	char* error = nullptr;
	char** results = NULL;
	int rows, columns;
	string select_max = "SELECT Distinct max(date) FROM PairOnePrices ";
	sqlite3_get_table(db, select_max.c_str(), &results, &rows, &columns, &error);
	string max_date;
	if (rows * columns > 0) max_date = results[1];
	else max_date = "NO_DATA";
	return max_date;
}

int PullPairDataToDB(vector<string> symbols, map<string, Stock> &stocks, sqlite3* db,bool isPairOne) {

	string sConfigFile = "config.csv";
	map<string, string> config_map = ProcessConfigData(sConfigFile);
	string url_daily_common = split(config_map["daily_url_common"], ',')[0];
	string start_date = split(config_map["start_date"], ',')[0];
	string end_date = split(config_map["end_date"], ',')[0];
	string api_token = split(config_map["api_token"], ',')[0];
	string last_date_in_db = get_latest_date(db);
	if (last_date_in_db != "NO_DATA") start_date = last_date_in_db;

	string url_request_daily;
	//global initiliation of curl before calling a function
	curl_global_init(CURL_GLOBAL_ALL);
	vector<string>::iterator itr;
	string symbol;
	for (itr = symbols.begin(); itr != symbols.end(); itr++) {
		symbol = *itr;
		Stock stock(symbol);
		string readBuffer_daily;
		url_request_daily = url_daily_common + symbol + ".US?" + "from=" + start_date + "&to=" + end_date + "&api_token=" + api_token + "&period=d&fmt=json";
		if (PullMarketData(url_request_daily, readBuffer_daily) != 0) {
			cout << "Pull Market Daily Data for " << symbol << "failed" << endl;
		}
		if (PopulateDailyTrades(readBuffer_daily, stock) != 0) {
			cout << "Populate Market Daily Data for " << symbol << "failed" << endl;
		}
		vector<DailyTrade> dailytrades;
		dailytrades = stock.GetDailyTrade();
		for (vector<DailyTrade>::iterator itr1 = dailytrades.begin(); itr1 != dailytrades.end(); itr1++) {
			char sql_Insert[512];
			if (isPairOne)sprintf(sql_Insert, "INSERT or REPLACE INTO PairOnePrices(symbol, date, open, high, low, close, adjusted_close, volume) VALUES(\"%s\", \"%s\", %f, %f, %f, %f, %f, %d)", symbol.c_str(), itr1->GetDate().c_str(), itr1->GetOpen(), itr1->GetHigh(), itr1->GetLow(), itr1->GetClose(), itr1->GetAdjustedClose(), itr1->GetVolume());
			else sprintf(sql_Insert, "INSERT or REPLACE INTO PairTwoPrices(symbol, date, open, high, low, close, adjusted_close, volume) VALUES(\"%s\", \"%s\", %f, %f, %f, %f, %f, %d)", symbol.c_str(), itr1->GetDate().c_str(), itr1->GetOpen(), itr1->GetHigh(), itr1->GetLow(), itr1->GetClose(), itr1->GetAdjustedClose(), itr1->GetVolume());
			if (ExecuteSQL(db, sql_Insert) == -1)
				return -1;

		}
		stocks[symbol] = stock;
		

	}
 	//cout << "successfully retrieve data" << endl;
	return 0;
}


