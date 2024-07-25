
// See https://aka.ms/new-console-template for more information
using System;
using System.Data;
using System.Threading.Tasks;
using System.Diagnostics;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

using System.Text.Json;
using System.Collections.Generic;
using System.Linq;

using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Relational;

using Telegram.Bot;
using Telegram.Bot.Types;
using MySqlX.XDevAPI.Common;
using System.Xml.Linq;
using Newtonsoft.Json;
using System.Net;
using System.Numerics;

namespace LuckyAnt
{
    internal class Program
    {
        //private static CIMTManagerAPI mManager = null;
        private static uint MT5_CONNECT_TIMEOUT = 1000; // Delay for 1 seconds before checking again
        private static int delay_time = 500; // Delay for 0.5 seconds before checking again
        private static DateTime default_time = new(2024, 1, 1);
        //private static string conn = "server = 68.183.177.155; uid = ctadmin; pwd = CTadmin!123; database = mt5-crm; port = 3306;";
        //private static string db_name = "mt5-crm";
        //local
        //private static string conn = "server = 127.0.0.1; uid = root; pwd = testtest; database = mt5-crm; port = 3306;";
        //private static string db_name = "mt5-crm";

        //Live
        //private static string conn = "server = 174.138.30.54; uid = wpadmin; pwd = pB3$81Ef5DDo; database = luckyant-mt5; port = 3306;";
        //private static string db_name = "luckyant-mt5";

        //Testing
        private static string conn = "server = 68.183.177.155; uid = ctadmin; pwd = CTadmin!123; database = luckyant-pamm; port = 3306; ConnectionTimeout = 500; DefaultCommandTimeout = 500; Pooling = true;";
        private static string db_name = "luckyant-pamm";

        private static string mode_type = "demo";
        private static long chatId = -4034138212;
        private static string telegramApiToken = "6740313709:AAEILXwPzjUtEJH343edziI_wuQqbTPQ8ew";
        private static string title_name = "Lucky Ant Program";
        //1000(1 second), 60,000 (1 minutes), 
        private static int expired_second = 60000 * 15; // 15 minutes
        //private static long lucky_ant_id = 2; // live - 7 
        //private static long lucky_ant_id = 7; // live - 7 

        static async Task Main()
        {
            Console.WriteLine("Current database:" + conn);
            Console.WriteLine("================================================================================");
            DateTime currentDate = DateTime.Now;
            currentDate = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day, 8, 0, 0);

            string input = await AwaitConsoleReadLine(1000);

            if (input == "Y" || input == "y" || input == null)
            {
                bool profit_sharing_progress = true;

                if (profit_sharing_progress == true)
                {
                    await proceed_profit_sharing();
                    await proceed_performance_incentive();
                }
            }
            else if (input == "N" || input == "n")
            {
                Console.WriteLine("operation cancelled!");
                return;
            }
            else
            {
                Console.WriteLine("Invalid input! Please try again!");
            }
            return;
        }

        private static async Task proceed_profit_sharing()
        {
            try
            {
                long num_subs = 0;
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string cnt_sub_sqlstr = $"SELECT COUNT(subscription_number) FROM pamm_subscriptions WHERE deleted_at IS NULL AND status = 'Active'; ";
                    Console.WriteLine($"cnt_sub_sqlstr: {cnt_sub_sqlstr}");
                    MySqlCommand select_cmd = new MySqlCommand(cnt_sub_sqlstr, sql_conn);
                    object result = select_cmd.ExecuteScalar();
                    if (result != null)
                    {
                        num_subs = Convert.ToInt64(result);
                    }
                }

                List<object[]> subs_List = new List<object[]>();
                List<object[]> exist_recordsList = new List<object[]>();
                List<ulong> trading_accList = new List<ulong>();
                string tradingacc_strsql = "0,";
                //Console.WriteLine($"num_subs: {num_subs}");
                if (num_subs > 0)
                {
                    // get subscription all info which are need to process
                    using (MySqlConnection sql_conn = new MySqlConnection(conn))
                    {
                        sql_conn.Open(); // Open the connection
                        string sub_sqlstr = $"SELECT t1.subscription_number, t1.user_id, t2.setting_rank_id, t1.meta_login, t1.id, t1.status, " +
                                            $"t1.subscription_amount, t1.settlement_period, t1.settlement_date, t1.master_id, t1.cumulative_amount, t1.max_out_amount, t1.termination_date " +
                                            $"FROM pamm_subscriptions t1 INNER JOIN users t2 ON t1.user_id = t2.id WHERE t2.deleted_at is null and t2.status = 'Active' " +
                                            $"AND t1.deleted_at IS NULL AND t1.status = 'Active' AND t1.max_out_amount is not null AND " +
                                            $"t1.settlement_date < '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' ORDER BY t1.subscription_number asc;";
                        //Console.WriteLine($"sub_sqlstr: {sub_sqlstr}");
                        MySqlCommand select_cmd = new MySqlCommand(sub_sqlstr, sql_conn);
                        MySqlDataReader reader = select_cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            //0-SubNum, 1-UserId, 2-UserRank, 3-MetaLogin, 4-SubId, 5-SubStatus, 6-SubAmount, 7-Settlement_Period, 8-Settlement_DateTime, 9-master_id, 10-cumulative_amt, 11-max_amt, 12-termination_date
                            object[] subsData = { reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetString(5), reader.GetDouble(6),
                                                reader.GetInt32(7),
                                                reader.IsDBNull(8) ? (object)default_time : reader.GetDateTime(8),
                                                reader.GetInt64(9), reader.GetDouble(10), reader.GetDouble(11),
                                                reader.IsDBNull(12) ? (object)default_time : reader.GetDateTime(12)
                                                };
                            trading_accList.Add((ulong)reader.GetInt64(3));
                            tradingacc_strsql += $", {reader.GetInt64(3)}";
                            subs_List.Add(subsData);
                        }
                        reader.Close();
                    }
                    Console.WriteLine($"subs_List.Count: {subs_List.Count}");
                    // is have more than 1 pamm_subscriptions which are need to process
                    if (subs_List.Count > 0)
                    {
                        //List<Subscription> pamm_subscriptions = GetSubValues();
                        bool isMonitorInitiliaze = true;
                        bool isSuccess = false;
                        int consecutiveFailures = 0;
                        CIMTManagerAPI mManager = null;
                        string s_comment = $"Settlement of Profit";

                        MTRetCode status = MTRetCode.MT_RET_OK_NONE;
                        // initialize MetaTradeAPI and get from MT5 info
                        while (isMonitorInitiliaze)
                        {
                            string remarks = "";
                            mManager = InitializeMetaTrader5API(out remarks, out status);
                            if (status != MTRetCode.MT_RET_OK)
                            {
                                Console.WriteLine($"Initialize failed : {status}");
                                mManager.Disconnect();
                                consecutiveFailures++;
                                if (consecutiveFailures >= 30)
                                {
                                    // Send a Telegram message
                                    await Telegram_Send("\n" + $"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures.. Exit out of profit sharing program");
                                    Console.WriteLine($"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures.. Exit out of profit sharing program");
                                    isMonitorInitiliaze = false; // Exit the loop
                                }
                            }
                            else
                            {
                                bool status_mt5 = false;
                                Console.WriteLine($"tradingacc_strsql: {tradingacc_strsql}");

                                // check MT5 account histroy is settement charged ? 
                                exist_records_MT5_Deal(mManager, trading_accList, s_comment, ref exist_recordsList, ref status_mt5);
                                if (status_mt5)
                                {
                                    foreach (var record in exist_recordsList)
                                    {
                                        Console.WriteLine($"record: {string.Join(", ", record)}");
                                    }
                                    isMonitorInitiliaze = false;
                                }
                            }
                            await Task.Delay(10000); // Wait for 10 seconds before checking again
                        }

                        if (isMonitorInitiliaze == false && status == MTRetCode.MT_RET_OK)
                        {
                            foreach (var subs in subs_List)
                            {
                                Console.WriteLine(" ");
                                //0-SubNum, 1-UserId, 2-UserRank, 3-MetaLogin, 4-SubId, 5-SubStatus, 6-RenewStatus, 7-Approval_DateTime, 8-Expired_DateTime, 9-master_id
                                Console.WriteLine($"subs info: {string.Join(", ", subs)}");
                                string SubNum = (string)subs[0];
                                long SubId = (long)subs[4];
                                string SubStatus = (string)subs[5];
                                long Metalogin = (long)subs[3];
                                long MasterId = (long)subs[9];
                                long UserRank = (long)subs[2];
                                long UserId = (long)subs[1];
                                double cumulative_amt = (double)subs[10];
                                double max_amt = (double)subs[11];
                                double sub_amt = (double)subs[6];
                                int settle_period = (int)subs[7];

                                DateTime SubTermination = (DateTime)subs[12];
                                DateTime SubSettle = (SubTermination == default_time) ? (DateTime)subs[8] : SubTermination;

                                //double RawProfit = 0; 
                                //double Swap = 0; 
                                double Profit = 0;
                                MTRetCode check_bal_status = mManager.UserBalanceCheck((ulong)Metalogin, true, out double balance_user, out double balance_history, out double credit_user, out double credit_history);
                                Profit = Math.Round(balance_user - sub_amt, 2);

                                double sharing_percent = 0; double market_percent = 0; double company_percent = 0; double personal_bonus_percent = 0;
                                retrieve_master_profit_percent(MasterId, ref sharing_percent, ref market_percent, ref company_percent, UserRank, ref personal_bonus_percent);
                                double total_100 = Math.Round(sharing_percent + market_percent + company_percent, 0);
                                Console.WriteLine($"total_100: {total_100} - UserRank: {UserRank} - personal_bonus_percent: {personal_bonus_percent}");
                                if (SubSettle < DateTime.Now && total_100 == 100) // && CountClosed > 0 && Profit > 0
                                {
                                    if (Profit > 0)
                                    {
                                        bool status_comment = false; long exist_deal_id = 0;// mt5 acc if no charged
                                        var existingTrade = exist_recordsList.FirstOrDefault(trade => (long)(ulong)trade[0] == Metalogin && (string)trade[1] == s_comment);
                                        if (existingTrade != null)
                                        {
                                            status_comment = (bool)existingTrade[2];
                                            exist_deal_id = (long)(ulong)existingTrade[5];
                                        }

                                        Console.WriteLine($"get subs: {string.Join(", ", subs)} ---- status_comment: {status_comment}");

                                        if (status_comment == false)
                                        {
                                            MTRetCode balstatus = mManager.DealerBalance((ulong)Metalogin, (Profit * -(sharing_percent / 100.00)), (uint)CIMTDeal.EnDealAction.DEAL_BALANCE, "Settlement of Profit", out ulong deal_id);
                                            // balstatus = MTRetCode.MT_RET_REQUEST_DONE;
                                            if (balstatus == MTRetCode.MT_RET_REQUEST_DONE)
                                            {
                                                //long deal_id = 99999;
                                                Console.WriteLine($"balance_user : {balance_user}");
                                                balance_user = balance_user - Profit;
                                                insert_update_subsciption_n_profit_hist(subs, Profit, sharing_percent, market_percent, company_percent, personal_bonus_percent, (long)deal_id);
                                            }
                                        }
                                        else
                                        {
                                            // then is transaction inside recorded, supposed status is updated   
                                            check_records_transactions_subscriptions_profit_histories(mManager, UserId, Metalogin, Profit, SubNum, exist_deal_id, subs, sharing_percent, market_percent, company_percent, personal_bonus_percent);
                                        }
                                        //Console.WriteLine($"get subs: {string.Join(", ", subs)}");
                                    }
                                    else
                                    {
                                        insert_update_subsciption_n_profit_hist(subs, 0, sharing_percent, market_percent, company_percent, personal_bonus_percent, 0);
                                    }
                                }
                                Console.WriteLine(" ");
                            } //
                        }
                        mManager.Disconnect();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }


        private static async Task proceed_performance_incentive()
        {
            try
            {
                long num_subs = 0;
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    // subsription_batches, count if terminated

                    string cnt_batch_sqlstr = $"SELECT COUNT(t1.id) FROM subscriptions_profit_histories t1 JOIN pamm_subscriptions t2 ON t1.subscription_id = t2.id " +
                        $"JOIN masters t3 ON t2.master_id = t3.id WHERE t1.total_profit != 0 AND t1.deleted_at IS NULL AND " +
                        $"t3.market_profit != 0 AND NOT EXISTS (SELECT 1 FROM performance_incentive t4 WHERE t4.profit_id = t1.id); ";
                    //string cnt_batch_sqlstr = $"SELECT COUNT(id) FROM subscriptions_profit_histories WHERE total_profit != 0 AND deleted_at IS NULL AND " +
                    //    $"subscription_id NOT IN (SELECT subscription_id FROM performance_incentive); ";
                    //Console.WriteLine($"cnt_incentive_sqlstr: {cnt_batch_sqlstr}");
                    MySqlCommand select_cmd = new MySqlCommand(cnt_batch_sqlstr, sql_conn);
                    object result = select_cmd.ExecuteScalar();
                    if (result != null)
                    {
                        num_subs = Convert.ToInt64(result);
                    }
                }

                List<object[]> subs_List = new List<object[]>();
                List<object[]> exist_recordsList = new List<object[]>();
                //Console.WriteLine($"num_subs: {num_subs}");
                if (num_subs > 0)
                {
                    using (MySqlConnection sql_conn = new MySqlConnection(conn))
                    {
                        sql_conn.Open(); // Open the connection

                        string sqlstr = $"SELECT t1.total_profit, t1.profit_bonus_percent, t1.user_rank, t1.user_id, t1.subscription_number, t1.subscription_id, t1.meta_login, " +
                            $"t2.settlement_date, t2.cumulative_amount, t2.max_out_amount, t1.id " +
                            $"FROM subscriptions_profit_histories t1 JOIN pamm_subscriptions t2 ON t1.subscription_id = t2.id JOIN masters t3 ON t2.master_id = t3.id " +
                            $"WHERE t1.total_profit != 0 AND " +
                            $"t3.market_profit != 0 AND NOT EXISTS (SELECT 1 FROM performance_incentive t4 WHERE t4.profit_id = t1.id);";
                        //string sqlstr = $"SELECT t1.total_profit, t1.profit_bonus_percent, t1.user_rank, t1.user_id, t1.subscription_number, t1.subscription_id, t1.meta_login, t2.expired_date " +
                        //    $"FROM subscriptions_profit_histories t1 JOIN pamm_subscriptions t2 ON t1.subscription_id = t2.id WHERE total_profit != 0 AND " +
                        //    $"subscription_id NOT IN (SELECT subscription_id FROM performance_incentive);";
                        //Console.WriteLine($"check_batch sqlstr : {sqlstr}");
                        MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                        MySqlDataReader reader = select_cmd.ExecuteReader();
                        while (reader.Read())
                        {                          
                            //0-profit, 1-personal_bonus_percent, 2-UserRank, 3-UserId, 4-SubNum, 5-SubId, 6-MetaLogin, 7-ExpiredDate, 8-CumulativeAmt, 9-MaxAmt
                            object[] batchData = { reader.GetDouble(0), reader.GetDouble(1), reader.GetInt64(2), reader.GetInt64(3),
                                reader.GetString(4), reader.GetInt64(5), reader.GetInt32(6), reader.GetDateTime(7), reader.GetDouble(8), reader.GetDouble(9), reader.GetUInt64(10) };

                            subs_List.Add(batchData);
                        }
                        reader.Close();
                    }

                    if (subs_List.Count > 0)
                    {
                        foreach (var subs in subs_List)
                        {
                            double profit = (double)subs[0];
                            double personal_bonus_percent = (double)subs[1];
                            long UserRank = (long)subs[2];
                            long UserId = (long)subs[3];
                            string SubNum = (string)subs[4];
                            long SubId = (long)subs[5];
                            int MetaLogin = (int)subs[6];
                            DateTime SettleDate = (DateTime)subs[7];
                            double cumulative_amt = (double)subs[8];
                            double max_amt = (double)subs[9];
                            ulong ProfitId = (ulong)subs[10];
                            //double market_profit = 0;

                            Console.WriteLine($"===================================== Subscription - {SubNum} =============================");
                            //retrieve_market_profit_percent(SubNum, ref market_profit);

                            if (profit != 0)
                            {
                                double leftoverProfit = 0;
                                if (personal_bonus_percent > 0)
                                {
                                    double remaining_profit = 0;
                                    using (MySqlConnection sql_conn = new MySqlConnection(conn))
                                    {
                                        sql_conn.Open();
                                        double personal_bonus_profit = Math.Round(profit * (personal_bonus_percent / 100), 2);
                                        remaining_profit = personal_bonus_profit;

                                        List<object[]> subscriptionList = new List<object[]>();
                                        string sql_amt = $"SELECT id, cumulative_amount, max_out_amount " +
                                            $"FROM pamm_subscriptions WHERE deleted_at is null and meta_login = {MetaLogin} " +
                                            $"and cumulative_amount < max_out_amount ORDER BY approval_date; ";

                                        //Console.WriteLine($"retrieve_sub_info sqlstr: {sql_amt}");
                                        MySqlCommand amt_cmd = new MySqlCommand(sql_amt, sql_conn);
                                        MySqlDataReader reader = amt_cmd.ExecuteReader();
                                        while (reader.Read())
                                        {
                                            object[] subscriptions = new object[] { reader.GetInt64(0), reader.GetDouble(1), reader.GetDouble(2) };
                                            subscriptionList.Add(subscriptions);
                                        }
                                        reader.Close();
                                        foreach (var subscription in subscriptionList)
                                        {
                                            SubId = (long)subscription[0];
                                            cumulative_amt = (double)subscription[1];
                                            max_amt = (double)subscription[2];

                                            double bonusToAdd = Math.Min(remaining_profit, max_amt - cumulative_amt); // 100, 1500 1450
                                            double personal_bonus_wallet = Math.Round(bonusToAdd * 0.7, 2);
                                            double personal_e_wallet = Math.Round(bonusToAdd - personal_bonus_wallet, 2);
                                            double new_amt = 0;

                                            string personalRemarks = "";
                                            if (remaining_profit > 0)
                                            {
                                                if (bonusToAdd > 0)
                                                {
                                                    if (cumulative_amt + bonusToAdd <= max_amt)
                                                    {
                                                        personalRemarks = $"Performance Incentive Bonus from {SubNum} of {personal_bonus_percent}% : ${Math.Round(bonusToAdd, 2)}";
                                                        new_amt = cumulative_amt + bonusToAdd;
                                                    }
                                                    else
                                                    {
                                                        personalRemarks = $"Performance Incentive Bonus from {SubNum} of {personal_bonus_percent}% : ${Math.Round(bonusToAdd, 2)} (Reached Max Amount)";
                                                        leftoverProfit = Math.Round(remaining_profit + cumulative_amt - max_amt, 2);
                                                        new_amt = max_amt;
                                                    }
                                                    //personalRemarks = cumulative_amt + bonusToAdd <= max_amt ?
                                                    //    $"Performance Incentive Bonus from {SubNum} of {personal_bonus_percent}% : ${Math.Round(bonusToAdd, 2)}" :
                                                    //    $"Performance Incentive Bonus from {SubNum} of {personal_bonus_percent}% : ${Math.Round(bonusToAdd, 2)} (Reached Max Amount)";

                                                    // Insert into wallet update logs
                                                    string bWalletRemark = $"Performance Incentive(bonus_wallet) => ${bonusToAdd}% * 70% = ${personal_bonus_wallet} {(cumulative_amt + bonusToAdd > max_amt ? "(Reached Max Amount)" : "")}";
                                                    insert_wallet_update_log(0, MetaLogin, UserId, personal_bonus_wallet, "bonus_wallet", "PerformanceIncentive", bWalletRemark, SubNum, "Performance Incentive");

                                                    string eWalletRemark = $"Performance Incentive(e_wallet) => ${bonusToAdd}% * 30% = ${personal_e_wallet} {(cumulative_amt + bonusToAdd > max_amt ? "(Reached Max Amount)" : "")}";
                                                    insert_wallet_update_log(0, MetaLogin, UserId, personal_e_wallet, "e_wallet", "PerformanceIncentive", eWalletRemark, SubNum, "Performance Incentive");

                                                    string sqlstr = $"UPDATE pamm_subscriptions SET cumulative_amount = ROUND({new_amt}, 2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                                             $"WHERE deleted_at is null and id = {SubId} and user_id = {UserId} " +
                                                             $"and id > 0";
                                                    Console.WriteLine($"insert_update_incentive_n_hist 1 sqlstr : {sqlstr}");
                                                    MySqlCommand updateSub_cmd = new MySqlCommand(sqlstr, sql_conn);
                                                    updateSub_cmd.ExecuteScalar();
                                                }
                                                else
                                                {
                                                    personalRemarks = $"No Performance Incentive Bonus from {SubNum} (Max Amount)";
                                                    //leftoverProfit = remaining_profit;
                                                }

                                                // add more columns for more details??? like upline_sub_id, downline_id, etc
                                                // personal_sub_id, leftover_bonus, current_sub_quota, final_bonus
                                                string sqlstr2 = $"INSERT INTO performance_incentive(user_id, user_rank, meta_login, personal_sub_id, subscription_id, subscription_number, profit_id, " +
                                                                $"subscription_profit_amt, personal_bonus_percent, personal_bonus_amt, leftover_bonus, current_sub_quota, final_bonus, " +
                                                                $"bonus_wallet_amt, e_wallet_amt, remarks, created_at) VALUES ( " +
                                                                $"{UserId}, {UserRank}, {MetaLogin}, {SubId}, {SubId}, '{SubNum}', {ProfitId}, {profit}, {personal_bonus_percent}," +
                                                                $" ROUND({remaining_profit},2), ROUND({leftoverProfit},2), ROUND({new_amt},2)," +
                                                                $" ROUND({bonusToAdd},2), ROUND({personal_bonus_wallet},2), ROUND({personal_e_wallet},2), " +
                                                                $"'{personalRemarks}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                                                //Console.WriteLine($"insert_update_incentive_n_hist 0 sqlstr : {sqlstr2}");
                                                MySqlCommand insert_cmd = new MySqlCommand(sqlstr2, sql_conn);
                                                insert_cmd.ExecuteScalar();
                                            }

                                            remaining_profit -= bonusToAdd;
                                            leftoverProfit = remaining_profit;
                                        }

                                    }
                                }

                                double lastPercent = personal_bonus_percent;
                                long lastRank = UserRank;
                                string upline = "";
                                long rank = 1;
                                MySqlConnection innerSql_conn = new MySqlConnection(conn);
                                innerSql_conn.Open();
                                string uplineQuery = "SELECT TRIM(BOTH ',' FROM REPLACE(hierarchyList, '-', ',')), setting_rank_id FROM users WHERE id = " + UserId + ";";
                                MySqlCommand uplineSelect_cmd = new MySqlCommand(uplineQuery, innerSql_conn);
                                MySqlDataReader innerReader = uplineSelect_cmd.ExecuteReader();
                                if (innerReader.Read())
                                {
                                    if (innerReader.IsDBNull(0))
                                    {
                                        Console.WriteLine("NO UPLINE");
                                    }
                                    else
                                    {
                                        upline = innerReader.GetString(0);
                                    }
                                    rank = innerReader.GetInt64(1);
                                }
                                innerSql_conn.Close();

                                if (upline != "")
                                {
                                    Console.WriteLine(" ---------------------------------- Upline ------------------");

                                    long[] upline_id = Array.ConvertAll(upline.Split(','), long.Parse);
                                    Console.WriteLine("Upline_ids: " + upline + " - " + upline_id.Length + " ids");
                                    for (int cnt = upline_id.Length - 1; cnt >= 0; cnt--)
                                    {
                                        List<object[]> rankCheck = new List<object[]>();
                                        List<object[]> toRemove = new List<object[]>();

                                        int rankDayDiff = 0;
                                        double affiliateEarning = 0;
                                        long uplineWallet = 0;
                                        double uplineBalance = 0;

                                        Console.WriteLine("Upline_ids: " + upline_id[cnt]);
                                        //
                                        innerSql_conn.Open();
                                        uplineQuery = $"SELECT setting_rank_id FROM users WHERE id = {upline_id[cnt]};";
                                        /*Console.WriteLine("uplineQuery: " + uplineQuery)*/
                                        
                                        uplineSelect_cmd = new MySqlCommand(uplineQuery, innerSql_conn);
                                        long uplineRank = (long)(ulong)uplineSelect_cmd.ExecuteScalar();
                                        innerSql_conn.Close();

                                        MySqlConnection innerSql_conn4 = new MySqlConnection(conn);
                                        innerSql_conn4.Open();
                                        string rankCheckQuery = $"SELECT old_rank from ranking_logs where user_id = {upline_id[cnt]} AND created_at > '{SettleDate.ToString("yyyy-MM-dd HH:mm:ss")}' order by created_at asc limit 1;";
                                        MySqlCommand rank_check = new MySqlCommand(rankCheckQuery, innerSql_conn4);
                                        //Console.WriteLine($"rankCheck str : {rankCheckQuery}");
                                        object result = rank_check.ExecuteScalar();
                                        if (result != null) { uplineRank = Convert.ToInt64(result); }
                                        innerSql_conn4.Close();

                                        using (MySqlConnection innerSql_conn2 = new MySqlConnection(conn))
                                        {
                                            innerSql_conn2.Open();

                                            string affiliateEarningQuery = $"SELECT profit_bonus_percent from setting_ranks where position = {uplineRank};";
                                            MySqlCommand select_cmd = new MySqlCommand(affiliateEarningQuery, innerSql_conn2);
                                            double uplinePercent = (double)select_cmd.ExecuteScalar();
                                            double actualPercent = uplinePercent - lastPercent;
                                            double upline_bonus_profit = Math.Round(profit * (actualPercent / 100), 2);
                                            double remaining_profit = (uplineRank == lastRank && leftoverProfit > 0) ? leftoverProfit : upline_bonus_profit + leftoverProfit;

                                            List<object[]> subscriptionList = new List<object[]>();
                                            string sql_amt = $"SELECT id, meta_login, cumulative_amount, max_out_amount " +
                                                $"FROM pamm_subscriptions WHERE deleted_at is null and user_id = {upline_id[cnt]} " +
                                                $"and cumulative_amount < max_out_amount ORDER BY approval_date; ";

                                            //Console.WriteLine($"retrieve_sub_info sqlstr: {sql_amt}");
                                            MySqlCommand amt_cmd = new MySqlCommand(sql_amt, innerSql_conn2);
                                            MySqlDataReader reader = amt_cmd.ExecuteReader();
                                            while (reader.Read())
                                            {
                                                object[] subscriptions = new object[] { reader.GetUInt64(0), reader.GetUInt64(1), reader.GetDouble(2), reader.GetDouble(3) };
                                                subscriptionList.Add(subscriptions);
                                            }
                                            reader.Close();

                                            foreach (var subscription in subscriptionList)
                                            {
                                                ulong uplineSub = (ulong)subscription[0];
                                                ulong uplineMtLogin = (ulong)subscription[1];
                                                double upline_cumulative_amt = (double)subscription[2];
                                                double upline_max_amt = (double)subscription[3];

                                                double bonusToAdd = Math.Min(remaining_profit, upline_max_amt - upline_cumulative_amt);
                                                double upline_bonus_wallet = Math.Round(bonusToAdd * 70 / 100, 2);
                                                double upline_e_wallet = Math.Round(bonusToAdd - upline_bonus_wallet, 2);
                                                double new_amt = 0;

                                                string uplineRemarks = "";
                                                if (remaining_profit > 0)
                                                {
                                                    if (bonusToAdd > 0)
                                                    {
                                                        if (cumulative_amt + bonusToAdd <= max_amt)
                                                        {
                                                            uplineRemarks = $"Performance Incentive Bonus from {SubNum} of {actualPercent}% : ${Math.Round(bonusToAdd, 2)}";
                                                            new_amt = cumulative_amt + bonusToAdd;
                                                        }
                                                        else
                                                        {
                                                            uplineRemarks = $"Performance Incentive Bonus from {SubNum} of {actualPercent}% : ${Math.Round(bonusToAdd, 2)} (Reached Max Amount)";
                                                            leftoverProfit = Math.Round(remaining_profit + cumulative_amt - max_amt, 2);
                                                            new_amt = max_amt;
                                                        }

                                                        // Insert into wallet update logs
                                                        string bWalletRemark = $"Performance Incentive(bonus_wallet) => ${bonusToAdd}% * 70% = ${upline_bonus_wallet} {(cumulative_amt + bonusToAdd > max_amt ? "(Reached Max Amount)" : "")}";
                                                        insert_wallet_update_log(0, MetaLogin, upline_id[cnt], upline_bonus_wallet, "bonus_wallet", "PerformanceIncentive", bWalletRemark, SubNum, "Performance Incentive");

                                                        string eWalletRemark = $"Performance Incentive(e_wallet) => ${bonusToAdd}% * 30% = ${upline_e_wallet} {(cumulative_amt + bonusToAdd > max_amt ? "(Reached Max Amount)" : "")}";
                                                        insert_wallet_update_log(0, MetaLogin, upline_id[cnt], upline_e_wallet, "e_wallet", "PerformanceIncentive", eWalletRemark, SubNum, "Performance Incentive");

                                                        string sqlstr = $"UPDATE pamm_subscriptions SET cumulative_amount = ROUND({new_amt}, 2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                                                    $"WHERE deleted_at is null and id = {uplineSub} and user_id = {upline_id[cnt]} " +
                                                                    $"and id > 0";
                                                        Console.WriteLine($"insert_update_incentive_n_hist 1 sqlstr : {sqlstr}");
                                                        MySqlCommand updateSub_cmd = new MySqlCommand(sqlstr, innerSql_conn2);
                                                        updateSub_cmd.ExecuteScalar();
                                                    }
                                                    else
                                                    {
                                                        uplineRemarks = $"No Performance Incentive Bonus from {SubNum} (Reached Max Amount)";
                                                    }

                                                    // add more columns for more details??? like upline_sub_id, downline_id, etc
                                                    // personal_sub_id, leftover_bonus, current_sub_quota, final_bonus
                                                    string sqlstr2 = $"INSERT INTO performance_incentive(user_id, user_rank, meta_login, personal_sub_id, subscription_id, subscription_number, profit_id, " +
                                                        $"subscription_profit_amt, personal_bonus_percent, personal_bonus_amt, leftover_bonus, current_sub_quota, final_bonus, " +
                                                        $"bonus_wallet_amt, e_wallet_amt, remarks, created_at) VALUES ( " +
                                                        $"{UserId}, {UserRank}, {uplineMtLogin}, {uplineSub}, {SubId}, '{SubNum}', {ProfitId}, {profit}, {actualPercent}," +
                                                        $" ROUND({upline_bonus_profit},2), ROUND({leftoverProfit},2), ROUND({new_amt},2)," +
                                                        $" ROUND({bonusToAdd},2), ROUND({upline_bonus_wallet},2), ROUND({upline_e_wallet},2), " +
                                                        $"'{uplineRemarks}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                                                    //Console.WriteLine($"insert_update_incentive_n_hist 0 sqlstr : {sqlstr2}");
                                                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr2, innerSql_conn2);
                                                    insert_cmd.ExecuteScalar();
                                                }
                                                remaining_profit -= bonusToAdd;
                                                leftoverProfit = remaining_profit;
                                            }
                                            lastPercent = uplinePercent;
                                            lastRank = uplineRank;
                                        }
                                        
                                    }

                                }
                            }
                            else
                            {
                                Console.WriteLine("This subscription has no profit.");
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine("No incentive is currently available.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static void check_records_transactions_subscriptions_profit_histories(CIMTManagerAPI mManager, long UserId, long Metalogin, double Profit, string SubNum, long exist_deal_id, object[] subs,
        double sharing_percent, double market_percent, double company_percent, double personal_bonus_percent)
        {
            long record_cnt = 0;
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                // transaction check only needed if profit != 0
                //string sqlstr = $"select count(*) from transactions where deleted_at is null and transaction_type = 'Deposit' and status = 'Success' and remarks = 'Settlement of Profit For {SubNum}'";
                //Console.WriteLine($"0  check  sqlstr : {sqlstr}");
                //MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                //object result = select_cmd.ExecuteScalar();
                //if (result != null){ record_cnt =  Convert.ToInt64(result); }

                //if(record_cnt == 0 && exist_deal_id > 0)
                //{
                //    MTRetCode check_bal_status = mManager.UserBalanceCheck(457282, false, out double balance_user, out double balance_history, out double credit_user, out double credit_history);  
                //    insert_withdraw_transaction(exist_deal_id, UserId, Metalogin, Profit, balance_user, SubNum, "Settlement of Profit" );
                //}
                // separate for checking, check sub prof first then transaction in update
                string sqlstr = $"select count(*) from subscriptions_profit_histories where deleted_at is null and subscription_number = '{SubNum}' and is_claimed = 'Claimed'";
                MySqlCommand select1_cmd = new MySqlCommand(sqlstr, sql_conn);
                object result1 = select1_cmd.ExecuteScalar();
                if (result1 != null) { record_cnt = Convert.ToInt64(result1); }
                if (record_cnt == 0 && exist_deal_id > 0)
                {
                    insert_update_subsciption_n_profit_hist(subs, Profit, sharing_percent, market_percent, company_percent, personal_bonus_percent, exist_deal_id);
                }
            }
        }

        private static void insert_withdraw_transaction(long dealId, long userId, ulong walletId, long metaLogin, double profit, double new_balance, string subNum, string type, string transaction_type, long log_id)
        {
            string sqlstr = "";
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection

                    // type should be Settlement of Profit, need wallet id, need new wallet amount, profit should shared profit amount (60%)
                    if (dealId == 0)
                    {
                        sqlstr = $"INSERT INTO transactions (user_id, category, transaction_type, fund_type, to_wallet_id, from_meta_login, amount, transaction_amount, " +
                             $"new_wallet_amount, status, comment, remarks, created_at) VALUES ( " +
                             $"{userId}, 'wallet', '{transaction_type}', 'RealFund', {walletId}, {metaLogin}, ROUND({profit},2), ROUND({profit},2), " +
                             $"ROUND({new_balance},2), 'Success', {log_id}, '{type} For {subNum}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' )";
                    }
                    else
                    {
                        sqlstr = $"INSERT INTO transactions (user_id, category, transaction_type, fund_type, to_wallet_id, from_meta_login, ticket, amount, transaction_amount, " +
                             $"new_wallet_amount, status, comment, remarks, created_at) VALUES ( " +
                             $"{userId}, 'wallet', '{transaction_type}', 'RealFund', {walletId}, {metaLogin}, {dealId}, ROUND({profit},2), ROUND({profit},2), " +
                             $"ROUND({new_balance},2), 'Success', {log_id}, '{type} For {subNum}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' )";
                    }


                    Console.WriteLine($"insert_withdraw_transaction 1 sqlstr : {sqlstr}");
                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insert_cmd.ExecuteScalar();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static void insert_update_subsciption_n_profit_hist(object[] sub_info, double profit, double sharing_percent, double market_percent, double company_percent, double personal_bonus_percent, long deal_id)
        {
            //0-SubNum, 1-UserId, 2-UserRank, 3-MetaLogin, 4-SubId, 5-SubStatus, 6-RenewStatus, 7-Approval_DateTime, 8-Expired_DateTime, 9-master_id
            long UserId = (long)sub_info[1];
            long UserRank = (long)sub_info[2];
            long MetaLogin = (long)sub_info[3];
            long SubId = (long)sub_info[4];
            long MasterId = (long)sub_info[9];
            int SubDays = (int)sub_info[7];
            DateTime SettleDate = (DateTime)sub_info[8];
            double cumulative_amt = (double)sub_info[10];
            double max_amt = (double)sub_info[11];
            string SubNum = (string)sub_info[0];
            double sharing_profit = 0; double market_profit = 0; double company_profit = 0; double personal_bonus_profit = 0;
            double bonus_wallet = 0; double e_wallet = 0;

            if (sharing_percent > 0) sharing_profit = Math.Round(profit * (sharing_percent / 100), 2);
            //if(market_percent > 0)  market_profit = profit*(market_percent/100);
            //if(company_percent > 0)  company_profit = profit*(company_percent/100);
            if (personal_bonus_percent > 0)
            {
                personal_bonus_profit = Math.Round(profit * (personal_bonus_percent / 100), 2);
                bonus_wallet = Math.Round(personal_bonus_profit * 70 / 100, 2);
                e_wallet = personal_bonus_profit - bonus_wallet;
            }

            string remarks = $"Profit: ${Math.Round(profit, 2)} | {sharing_percent}% : ${Math.Round(sharing_profit, 2)} | {personal_bonus_percent}% : ${Math.Round(personal_bonus_profit, 2)} ";
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection

                    string sqlstr = $"INSERT INTO subscriptions_profit_histories(user_id, meta_login, user_rank, subscription_id, subscription_number, total_profit, profit_sharing_percent, " +
                                    $" profit_sharing_amt, profit_bonus_percent, profit_bonus_amt, is_claimed, claimed_datetime, remarks, created_at) VALUES ( " +
                                    $"{UserId}, {MetaLogin}, {UserRank}, {SubId}, '{SubNum}', ROUND({profit},2), {sharing_percent}, ROUND({sharing_profit},2), {personal_bonus_percent}, " +
                                    $" ROUND({personal_bonus_profit},2), " +
                                    $"'Claimed', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}', '{remarks}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";

                    //Console.WriteLine($"insert_update_subsciption_n_profit_hist 0 sqlstr : {sqlstr}");
                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insert_cmd.ExecuteScalar();                  

                    if (profit != 0)
                    {
                        //
                        double remaining_profit = sharing_profit;
                        List<object[]> subscriptionList = new List<object[]>();
                        string sql_amt = $"SELECT id, cumulative_amount, max_out_amount " +
                            $"FROM pamm_subscriptions WHERE deleted_at is null and meta_login = {MetaLogin} " +
                            $"and cumulative_amount != max_out_amount ORDER BY approval_date; ";

                        //Console.WriteLine($"retrieve_sub_info sqlstr: {sql_amt}");
                        MySqlCommand amt_cmd = new MySqlCommand(sql_amt, sql_conn);
                        MySqlDataReader reader = amt_cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            object[] subscriptions = new object[] { reader.GetInt64(0), reader.GetDouble(1), reader.GetDouble(2) };
                            subscriptionList.Add(subscriptions);
                        }
                        reader.Close();

                        foreach (var subscription in subscriptionList)
                        {
                            SubId = (long)subscription[0];
                            cumulative_amt = (double)subscription[1];
                            max_amt = (double)subscription[2];

                            double bonusToAdd = Math.Min(remaining_profit, max_amt - cumulative_amt);
                            double new_amt = 0;
                            string WalletRemark = "";
                            if (remaining_profit > 0)
                            {
                                if (bonusToAdd > 0)
                                {
                                    if (cumulative_amt + bonusToAdd <= max_amt)
                                    {
                                        WalletRemark = $"Profit Sharing(cash_wallet) => ${profit} * {sharing_percent}% = ${sharing_profit}";
                                        new_amt = cumulative_amt + bonusToAdd;
                                    }
                                    else
                                    {
                                        WalletRemark = $"Profit Sharing(cash_wallet) => ${profit} * {sharing_percent}% = ${sharing_profit} (Reached Max Amount)";
                                        new_amt = max_amt;
                                    }

                                    insert_wallet_update_log(deal_id, MetaLogin, UserId, bonusToAdd, "cash_wallet", "ProfitSharing", WalletRemark, SubNum, "Settlement of Profit");

                                    string sqlstr_amt = $"UPDATE pamm_subscriptions SET cumulative_amount = ROUND({new_amt}, 2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                             $"WHERE deleted_at is null and id = {SubId} and user_id = {UserId} " +
                                             $"and id > 0";
                                    //Console.WriteLine($"insert_update_incentive_n_hist 1 sqlstr : {sqlstr_amt}");
                                    MySqlCommand updateSubAmt_cmd = new MySqlCommand(sqlstr_amt, sql_conn);
                                    updateSubAmt_cmd.ExecuteScalar();
                                }
                            }
                            remaining_profit -= bonusToAdd;
                        }
                    }

                    bool subStatus = false;

                    string chkSettle_str = $"SELECT cumulative_amount, max_out_amount " +
                            $"FROM pamm_subscriptions WHERE deleted_at is null and subscription_number = '{SubNum}'";
                    MySqlCommand chkSettle_cmd = new MySqlCommand(chkSettle_str, sql_conn);
                    MySqlDataReader stReader = chkSettle_cmd.ExecuteReader();
                    if (stReader.Read())
                    {
                        if (stReader.GetDouble(0) >= stReader.GetDouble(1))
                        {
                            subStatus = true;
                        }
                    }
                    stReader.Close();

                    if (subStatus == true)
                    {
                        sqlstr = $"UPDATE pamm_subscriptions SET status = 'Maturity', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                             $"WHERE deleted_at is null and subscription_number = '{SubNum}' and user_id = {UserId} " +
                             $"and settlement_date < '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}';";
                    }
                    else
                    {
                        DateTime newDate = SettleDate.AddDays(SubDays);
                        sqlstr = $"UPDATE pamm_subscriptions SET settlement_date = '{newDate.ToString("yyy-MM-dd HH:mm:ss")}', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                             $"WHERE deleted_at is null and subscription_number = '{SubNum}' and user_id = {UserId} " +
                             $"and settlement_date < '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}';";
                    }

                    //Console.WriteLine($"insert_update_subsciption_n_profit_hist 2 sqlstr : {sqlstr}");
                    MySqlCommand updateSub_cmd = new MySqlCommand(sqlstr, sql_conn);
                    updateSub_cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }

        }

        private static void exist_records_MT5_Deal(CIMTManagerAPI mManager, List<ulong> metaLogin, string s_keyword, ref List<object[]> exist_trade, ref bool status_mt5)
        {
            Console.WriteLine($"retrieve_MT5_Deal ... ");
            try
            {
                DateTime last_date = DateTime.Now.AddDays(-2);
                long server_timestamp = 0;
                mManager.TimeServerRequest(out server_timestamp);
                DateTime current_date = DateTimeOffset.FromUnixTimeMilliseconds(server_timestamp).DateTime.AddDays(2);

                ulong[] trading_accounts = metaLogin.ToArray();

                // Implement logic for retrieve_trades_fromMT5 
                if (trading_accounts.Length > 0)
                {
                    Console.WriteLine("");
                    Console.WriteLine($" trading_accounts.Length: {trading_accounts.Length} ");
                    CIMTDealArray tradingAcc_Deals = mManager.DealCreateArray();
                    //CIMTDealArray tradingAcc_Deals00 = mManager.DealCreateArray();

                    MTRetCode res1 = mManager.DealRequestByLogins(trading_accounts, SMTTime.FromDateTime(last_date), SMTTime.FromDateTime(current_date), tradingAcc_Deals);
                    if (res1 == MTRetCode.MT_RET_ERR_NOTFOUND)
                    {
                        //Console.WriteLine($"tradingAcc_Deals - total: 0 -- {MTRetCode.MT_RET_ERR_NOTFOUND}");
                        status_mt5 = true;

                    }
                    else if (res1 != MTRetCode.MT_RET_OK)
                    {
                        Console.WriteLine("tradingAcc_Deals total- {0} - flag: {1}", tradingAcc_Deals.Total(), res1);
                    }
                    else
                    {
                        Console.WriteLine("saved tradingAcc_Deals : " + tradingAcc_Deals);
                        status_mt5 = true; long Count = 1;

                        for (uint i = 0; i < tradingAcc_Deals.Total(); i++)
                        {
                            CIMTDeal m_Deal = tradingAcc_Deals.Next(i);
                            if (m_Deal == null) { break; }

                            ulong Login = m_Deal?.Login() ?? 0;
                            ulong Deal = m_Deal?.Deal() ?? 0;
                            uint Action = m_Deal?.Action() ?? 0;
                            long Time = m_Deal?.Time() ?? 0;
                            DateTime dt_Timestamp = DateTimeOffset.FromUnixTimeSeconds(Time).UtcDateTime;
                            double Profit = m_Deal?.Profit() ?? 0;
                            string Commment = m_Deal?.Comment() ?? "";

                            if (Commment.Contains(s_keyword))
                            {
                                var existingTrade = exist_trade.FirstOrDefault(trade => (ulong)trade[0] == Login && (string)trade[1] == s_keyword);
                                if (existingTrade != null)
                                {
                                    // Update existing entry
                                    if ((bool)existingTrade[2] == false) existingTrade[2] = true;
                                    existingTrade[3] = ((long)Count) + 1;
                                    existingTrade[4] = ((double)existingTrade[4]) + Profit;
                                }
                                else
                                {
                                    // Add new entry
                                    object[] tradeData = { Login, Commment, true, Count, Profit, Deal };
                                    exist_trade.Add(tradeData);
                                }
                                Console.WriteLine($"Deal: {Deal} - Action: {Action} - dt_Timestamp: {dt_Timestamp.ToString("yyyy-MM-dd HH:mm:ss")} - Profit: {Profit} - Commment: {Commment}");
                            }

                            m_Deal.Release();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static void check_fund(string SubNum, ref double demo_fund)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $" SELECT COALESCE(SUM(demo_fund), 0) FROM subscription_batches WHERE subscription_number = '{SubNum}' AND deleted_at IS NULL;";

                    //Console.WriteLine($"check_demo_fund sqlstr: {sqlstr}");
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    demo_fund = (double)(decimal)select_cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static void updateAcc(int meta_login, double demo_fund)
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open();
                string sqlstr = $"UPDATE trading_accounts SET demo_fund = demo_fund - ROUND({demo_fund},2) " +
                             $"WHERE deleted_at is null AND meta_login = {meta_login} AND demo_fund IS NOT NULL;";
                Console.WriteLine($"insert_wallet_update_log 0 sqlstr : {sqlstr}");
                MySqlCommand updateDemoFund_cmd = new MySqlCommand(sqlstr, sql_conn);
                updateDemoFund_cmd.ExecuteScalar();

            }
        }

        private static void retrieve_master_profit_percent(long masterid, ref double sharing_profit, ref double market_profit, ref double company_profit, long user_rank, ref double personal_bonus)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $" SELECT COALESCE(sharing_profit,0), COALESCE(market_profit,0), COALESCE(company_profit,0) FROM masters WHERE deleted_at is null and id = {masterid}; ";

                    //Console.WriteLine($"retrieve_master_profit_percent sqlstr: {sqlstr}");
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        sharing_profit = reader.GetDouble(0);
                        market_profit = reader.GetDouble(1);
                        company_profit = reader.GetDouble(2);
                    } 
                    reader.Close();

                    string personal_sqlstr = $" SELECT COALESCE(profit_bonus_percent,0) FROM setting_ranks WHERE deleted_at is null and position = {user_rank}; ";
                    //Console.WriteLine($"retrieve_master_profit_percent personal_sqlstr: {personal_sqlstr}");
                    MySqlCommand select_cmd1 = new MySqlCommand(personal_sqlstr, sql_conn);
                    personal_bonus = (double)select_cmd1.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static void insert_wallet_update_log(long deal_id, long meta_login, long user_id, double amount, string type, string purpose, string remark, string sub_numb, string comment)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    ulong WalletId = 0;
                    double WalletBalance = 0.00;
                    double new_balance = 0.00;
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT id, COALESCE(balance,0) FROM wallets WHERE deleted_at is null and user_id = {user_id} and type = '{type}'; ";

                    //Console.WriteLine($"retrieve_master_profit_percent sqlstr: {sqlstr}");
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        WalletId = reader.GetUInt64(0);
                        WalletBalance = reader.GetDouble(1);
                        new_balance = Math.Round(WalletBalance + amount, 2);
                    }
                    reader.Close();

                    sqlstr = $"UPDATE wallets SET balance = balance + ROUND({amount},2) " +
                             $"WHERE deleted_at is null AND type = '{type}' AND user_id = {user_id};";
                    //Console.WriteLine($"insert_wallet_update_log 0 sqlstr : {sqlstr}");
                    MySqlCommand updateWallet_cmd = new MySqlCommand(sqlstr, sql_conn);
                    updateWallet_cmd.ExecuteScalar();

                    if (deal_id == 0)
                    {
                        sqlstr = $"INSERT INTO wallet_logs(user_id, wallet_id, old_balance, new_balance, wallet_type, category, purpose, amount, remark, created_at) VALUES ( " +
                           $"{user_id}, {WalletId}, {WalletBalance}, {new_balance}, '{type}', 'bonus', '{purpose}', ROUND({amount},2), '{remark}', " +
                           $"'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                    }
                    else
                    {
                        sqlstr = $"INSERT INTO wallet_logs(user_id, wallet_id, old_balance, new_balance, wallet_type, category, purpose, amount, remark, ticket, created_at) VALUES ( " +
                           $"{user_id}, {WalletId}, {WalletBalance}, {new_balance}, '{type}', 'bonus', '{purpose}', ROUND({amount},2), '{remark}', {deal_id}, " +
                           $"'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                    }

                    Console.WriteLine($"insert_wallet_update_log 1 sqlstr : {sqlstr}");
                    MySqlCommand insertWallet_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insertWallet_cmd.ExecuteScalar();

                    long logId = insertWallet_cmd.LastInsertedId;

                    insert_withdraw_transaction((long)deal_id, user_id, WalletId, meta_login, amount, new_balance, sub_numb, comment, purpose, logId);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static async Task Telegram_Send(string messages)
        {
            //Console.WriteLine("Enter Telegram_Send - "+messages);
            string telegramApiToken_0 = telegramApiToken;
            long chatId_0 = chatId;
            var botClient = new TelegramBotClient(telegramApiToken_0);
            //var me = await botClient.GetMeAsync();
            //Console.WriteLine($"Hello, World! I am user {me.Id} and my name is {me.FirstName}.");
            //Console.WriteLine(" Telegram_Send "+botClient );
            Console.WriteLine(" Telegram_Send " + (title_name + messages));

            try
            {
                await botClient.SendTextMessageAsync(chatId_0, (title_name + messages));
                Console.WriteLine("Message sent successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static CIMTManagerAPI InitializeMetaTrader5API(out string str, out MTRetCode res)
        {
            string serverName = "103.21.90.162";
            int server_port = 443;
            ulong adminLogin = 3001;
            string adminPassword = "CkVw+fQ8";
            CIMTManagerAPI mManager = null;
            str = "";

            MTRetCode ret = MTRetCode.MT_RET_OK_NONE;
            res = SMTManagerAPIFactory.Initialize(@"Libs\'MetaQuotes.MT5ManagerAPI64.dll"); ;
            if (res != MTRetCode.MT_RET_OK)
            {
                str = string.Format("Initialize error ({0})", ret);
                return mManager;
            }
            //Console.WriteLine($"Part 2"); 
            mManager = SMTManagerAPIFactory.CreateManager(SMTManagerAPIFactory.ManagerAPIVersion, out ret);
            if (ret == MTRetCode.MT_RET_OK)
            {
                res = mManager.Connect(serverName, adminLogin, adminPassword, null, CIMTManagerAPI.EnPumpModes.PUMP_MODE_FULL, MT5_CONNECT_TIMEOUT);
                if (res != MTRetCode.MT_RET_OK)
                {
                    str = string.Format("UserAccountRequest error ({0})", res);
                    return mManager;
                }
            }
            return mManager;
        }

        private static async Task<string> AwaitConsoleReadLine(int timeoutms)
        {
            Task<string> readLineTask = Task.Run(() => Console.ReadLine());

            if (await Task.WhenAny(readLineTask, Task.Delay(timeoutms)) == readLineTask)
            {
                return readLineTask.Result;
            }
            else
            {
                Console.WriteLine("Timeout!");
                return null;
            }
        }

        private static string ConvertListToString(List<ulong> list)
        {
            // Use string.Join to concatenate the ulong values with commas
            return string.Join(",", list);
        }

        //private static List<Subscription> GetSubValues()
        //{
        //    string json = new WebClient().DownloadString("http://103.21.90.87:8080/serverapi/pamm/subscriber-list/24");

        //    List<Subscription> subscriptions = JsonConvert.DeserializeObject<List<Subscription>>(json);

        //    foreach (var subscription in subscriptions)
        //    {
        //        Console.WriteLine("ID: " + subscription.id);
        //        Console.WriteLine("User ID: " + subscription.user_id);
        //        Console.WriteLine("Trading Account ID: " + subscription.trading_account_id);
        //        Console.WriteLine("Meta Login: " + subscription.meta_login);
        //        Console.WriteLine("Sub Amount (USD): " + subscription.subscription_amount);
        //        Console.WriteLine("master ID: " + subscription.master_id);
        //        Console.WriteLine("Subscription Number: " + subscription.subscription_number.ToUpper());
        //        Console.WriteLine("Status: " + subscription.status);
        //        Console.WriteLine("\n");
        //    }

        //    return subscriptions;
        //}
    }

    //public class Subscription
    //{
    //    public long id { get; set; }
    //    public long user_id { get; set; }
    //    public long trading_account_id { get; set; }
    //    public long meta_login { get; set; }
    //    public double subscription_amount { get; set; }
    //    public long master_id { get; set; }
    //    public string subscription_number { get; set; }
    //    public string status { get; set; }
    //}
}
