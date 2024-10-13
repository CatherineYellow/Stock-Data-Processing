# Stock-Data-Processing
## I. Problem Description

For the provided Order Record and Trade Record tables, there is an issue where the order type (OrderType) field cannot distinguish the type of market order, which leads to difficulties in further analysis. To resolve this problem, we will:

1. **Organize the Order and Trade Tables**:
   - Mark price for limit orders.
   - Mark levels for market orders.

2. **Sorting**:
   - **Order Record Sorting**: Sort order records (market orders, limit orders, self-best orders) by timestamp and order index to ensure more accurate tracking of order execution and trade status.
   - **Cancellation Record Sorting**: Sort cancellation records by timestamp and original data order to better restore the cancellation process, ensuring consistency and reliability for further analysis.

## II. Task Understanding

1. **Method Selection**: The task requires the use of HDFS + MapReduce to implement the solution.
   - **HDFS**: Transfer relevant files into HDFS using Docker for input/output.
   - **MapReduce**: Design and implement the corresponding Mapper and Reducer to address the issues in the Order Record and Trade Record tables.

2. **Field Sources**: Output a table with 7 fields based on the Order Record and Trade Record tables:

   - `TIMESTAMP`: The time of the order or cancellation execution. This is determined by the `ExecType` field in the trade record, and the timestamp is taken from the `TransactTime` or `TradeTime` fields.
   
   - `PRICE`: Records the price for limit orders, taken directly from the `Price` field in the order table.
   
   - `BUY_SELL_FLAG`: Records the direction of the order (1-buy, 2-sell), determined from the `Side` field in the order table and by checking the non-zero fields in `BidApplSeqNum` and `OfferApplSeqNum` in the trade table.
   
   - `ORDER_TYPE`: The type of the order (1-limit order, 2-market order, U-self-best order), taken directly from the `OrderType` field in the order table.
   
   - `ORDER_ID`: A unique identifier for the order, extracted from the `ApplSeqNum` field. For cancellations, it takes the non-zero values from the `BidApplSeqNum` and `OfferApplSeqNum` fields.
   
   - `MARKET_ORDER_TYPE`: Records the number of price levels for market orders. This is calculated by matching trade records with the order records using `ApplSeqNum`. A `HashSet` or similar data structure counts the number of distinct prices for the same `ApplSeqNum`, excluding cancellations.
   
   - `CANCEL_TYPE`: Indicates whether the order is canceled (1-canceled, 2-not canceled).

3. **Overall Task**: Output all order records and cancellation records from the trade record table, sorted by `TIMESTAMP`. For the same `TIMESTAMP`, record orders before cancellations. If there are multiple order or cancellation records, sort by `ORDER_ID`.

## III. Difficulties and Challenges

1. **Field Complexity**: The large number of irrelevant fields in both tables makes it difficult to filter and organize the necessary data. Field cleanup is required to simplify the process.
   
2. **Missing Data**:
   - Cancellations without an order record should still be recorded.
   - Trades without an order record should not be recorded.
   - Orders without trade records should still be recorded.

3. **Background Knowledge**: Stock market-related knowledge is needed to understand the problem correctly.

4. **Data Issues**: The data is not entirely clean, with noise such as trade records during pre-market auctions that may confuse the analysis.

## IV. Overall Technical Plan

1. **Process**: The solution is divided into three phases: Map, Reduce, and Sort.

2. **Case Handling**: Handle the following four cases:

| TIMESTAMP   | PRICE | SIZE    | BUY_SELL_FLAG | ORDER_TYPE       | ORDER_ID     | MARKET_ORDER_TYPE | CANCEL_TYPE |
| ----------- | ----- | ------- | ------------- | ---------------- | ------------ | ----------------- | ----------- |
| Order Time  | null  | Order Size | 1-buy, 2-sell | **Market 1**     | Order Index  | 0,1,2,...         | 2           |
| Order Time  | Price | Order Size | 1-buy, 2-sell | **Limit 2**      | Order Index  | null              | 2           |
| Order Time  | null  | Order Size | 1-buy, 2-sell | **Self-best U**  | Order Index  | null              | 2           |
| Cancel Time | null  | Trade Size | 1-buy, 2-sell | **For ease mark 4** | Non-zero index | null          | 1           |

4. **Field Sources**:
   - Cancellation data comes entirely from the trade table.
   - Limit orders and self-best orders come entirely from the order table.
   - For market orders, the levels come from the trade table; other fields come from the order table.

5. **Process**:
   - **Map**: The order and trade records are loaded from different Mappers. Only records within continuous trading time and for the selected stock (Ping An Bank) are processed; others are filtered out.
   - **Reduce**: If a cancellation is present, record the cancellation; if an order record is present, record the order. Limit orders and self-best orders can be directly recorded from the order table, while market orders require matching trade records to determine the number of levels.
   - **Sort**: Sort by timestamp in ascending order, then by cancellation type in descending order, and finally by order index in ascending order. The header should contain column names.

## V. Code Modular Design

1. **Map**:
   - **Mapper1**: Process the `order` table:
   
     ```java
     Mapper1: Use order index as key
     1. Filter by continuous trading time and Ping An Bank.
     2. Extract relevant fields: "O, Order Time, Price, Order Size, Buy/Sell Flag, Order Type".
     ```

   - **Mapper2**: Process the `trade` table:
   
     ```java
     Mapper2: Use buy or sell order index as key
     1. Filter by continuous trading time and Ping An Bank.
     2. Extract relevant fields: "T, Trade Time, Trade Price, Cancellation Flag, Buy/Sell Flag, Trade Size".
     ```

2. **Reduce**:
   
   ```java
   Reducer: 
   1. Iterate through values:
      (a) Store order and trade tables.
      (b) Handle cancellations.
   
   2. If no order record exists: skip.
   
   3. Based on the order type (OrderType O5), determine trade type:
      (a) Limit: Directly write from order table fields.
      (b) Market:
          - Add trade prices to HashSet.
          - If HashSet is empty: Write type as 0.
          - If not empty: Write type as HashSet size.
      (c) Self-best: Directly write from order table fields.
3. **Sorting**:
Use Java’s built-in sorting with a custom `compare` method to sort by timestamp in ascending order, cancellation type in descending order, and order index in ascending order.

## VI. HDFS Usage

1. Upload the files to HDFS under `/project/data`.

2. Transfer the JAR file to Docker.

3. Run the command:

   ```java
   hadoop jar project-1.0-SNAPSHOT.jar driver.StockAnalysisDriver /project/data/order/am_hq_order_spot.txt /project/data/order/pm_hq_order_spot.txt /project/data/trade/am_hq_trade_spot.txt /project/data/trade/pm_hq_trade_spot.txt /project/output /project/output/part-r-00000
   ```
4. To re-run the process, remove the output directory:
```java
   hdfs dfs -rm -r /project/output
   ```