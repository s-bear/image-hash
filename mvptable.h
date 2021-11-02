#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <functional>

class SQLStatementCache
{
	//The database connection
	std::shared_ptr<SQLite::Database> db;
	/* Cache for compiled SQL statememts */
	std::unordered_map<std::string, SQLite::Statement> stmts;

public:
	SQLStatementCache();
	SQLStatementCache(std::shared_ptr<SQLite::Database> db);

	SQLite::Statement& get(const std::string& stmt);
	
	inline SQLite::Statement& operator[](const std::string& stmt) {
		return get(stmt);
	}

	void exec(SQLite::Statement& stmt);
	void exec(const std::string& stmt);

	int32_t exec_getInt(SQLite::Statement& stmt, int column);
	int64_t exec_getInt64(SQLite::Statement& stmt, int column);
	std::string exec_getString(SQLite::Statement& stmt, int column);
	double exec_getDouble(SQLite::Statement& stmt, int column);
	std::vector<uint8_t> exec_getBlob(SQLite::Statement& stmt, int column);

	int32_t exec_getInt(const std::string& stmt, int column);
	int64_t exec_getInt64(const std::string& stmt, int column);
	std::string exec_getString(const std::string& stmt, int column);
	double exec_getDouble(const std::string& stmt, int column);
	std::vector<uint8_t> exec_getBlob(const std::string& stmt, int column);

	int32_t exec_getInt(SQLite::Statement& stmt, const std::string& column);
	int64_t exec_getInt64(SQLite::Statement& stmt, const std::string& column);
	std::string exec_getString(SQLite::Statement& stmt, const std::string& column);
	double exec_getDouble(SQLite::Statement& stmt, const std::string& column);
	std::vector<uint8_t> exec_getBlob(SQLite::Statement& stmt, const std::string& column);

	int32_t exec_getInt(const std::string& stmt, const std::string& column);
	int64_t exec_getInt64(const std::string& stmt, const std::string& column);
	std::string exec_getString(const std::string& stmt, const std::string& column);
	double exec_getDouble(const std::string& stmt, const std::string& column);
	std::vector<uint8_t> exec_getBlob(const std::string& stmt, const std::string& column);
};

class MVPTable
{
	//TODO: there's an annoying snag in the interface here, if this is ever to be generalized more
	//  specifically marshalling between blobs and the point's value
	//  it doesn't matter much in this application because they're both just vector<byte>
public:
	using blob_type = std::vector<uint8_t>;
	using distance_fn = int32_t (const blob_type&, const blob_type&);

	MVPTable();

	// Init with an open database
	// No transaction
	explicit MVPTable(std::shared_ptr<SQLite::Database> db, std::function<distance_fn> dist_fn);

	// Insert a point into mvp_points if it doesn't already exist
	//  Each point is stored with the distances of the point to each vantage point
	//  And which partition it falls into
	// No transaction
	// Returns the id of the point
	int64_t insert_point(const blob_type& p_value);

	//how many points are there (cached)?
	int64_t count_points();
	int64_t count_vantage_points();

	// Insert a point into mvp_vantage_points
	// Throws a std::runtime_error if the point already exists
	// Adds a new "d{id}" column to mvp_points and fills it with the distance
	//   from each point to vp_value
	// Recomputes the mvp_points partition, balancing only the new vantage point
	// No transaction
	// Returns the id of the vantage point
	int64_t insert_vantage_point(const blob_type& vp_value);

	// Get point ids within `radius` of `q_value`
	// The results (id, dist) are stored in the temp.mvp_query table
	// Returns the number of points found
	int64_t query(const blob_type& q_value, uint32_t radius);

	// Find a point that would make a good vantage point
	blob_type find_vantage_point(size_t sample_size);

	// Get ids of vantage points that are imbalanced beyond the threshold
	// threshold must be in 0.0 to 1.0
	// if there are fewer than min_count points in the database, does nothing
	std::vector<int64_t> check_balance(int64_t min_count = 50, float threshold = 0.5f);
	
	// Balance the given vantage point
	void balance(int64_t vp_id);

	//balance(id) for each id in check_balance(threshold);
	void auto_balance(int64_t min_count = 50, float threshold = 0.5f);

	// insert vantage points using find_vantage_point
	// until the number of vantage points meets or exceeds log(count_points()) / log(4 * target)
	int64_t auto_vantage_point(int64_t target = 100);

protected:

	void check_db();

	//each vantage point gets 2 bits of the partition, indexed by its id
	constexpr int64_t partition_offset(int64_t vp_id) { return 2 * (vp_id - 1); }
	constexpr int64_t partition_mask() { return 0x3; }

	constexpr int64_t partition_mask(int64_t vp_id) {
		return partition_mask() << partition_offset(vp_id);
	}
	constexpr int64_t partition_bits(int64_t shell, int64_t vp_id) {
		return shell << partition_offset(vp_id);
	}

	static void set_dist_fn(std::function<distance_fn> df);
	static std::function<distance_fn> dist_fn;
	// callback for "mvp_distance" sql function
	//   args are 2 point values, as blobs
	//   returns dist_fn(args[0], args[1])
	static void sql_distance(sqlite3_context* ctx, int n, sqlite3_value* args[]);
	
	//blob to vector
	static blob_type get_blob(sqlite3_value* val);
	static blob_type get_blob(SQLite::Column& col);

	// Update cached vp_ids
	// Deletes ins_point if vp_ids has changed
	void update_vp_ids(const std::vector<int64_t>& vp_ids);

	//INSERT INTO mvp_points(part, value, d0, d1, ...) VALUES ($part, $value, $d0, $d1, ...) RETURNING id;
	//where d0, d1, ... are "d{id}" for id in vp_ids
	static const std::string str_ins_point(const std::vector<int64_t>& vp_ids);

	//INSERT INTO temp.mvp_query(id, dist)
	//  SELECT id, mvp_distance($q_value, value) AS dist
	//    FROM mvp_points WHERE partition = $partition AND dist <= $radius;
	static const std::string str_ins_query(const std::vector<int64_t>& vp_ids);

	//The database connection
	std::shared_ptr<SQLite::Database> db;
	SQLStatementCache cache;
	
	// we don't cache these statements because they aren't static

	//INSERT INTO mvp_points(part, value, d0, d1, ...) VALUES ($part, $value, $d0, $d1, ...) RETURNING id;
	//where d0, d1, ... are "d{id}" for id in vp_ids
	std::unique_ptr<SQLite::Statement> ins_point;

	//INSERT INTO temp.mvp_query(id, dist)
	//  SELECT id, mvp_distance($q_value, value) FROM mvp_points
	//    WHERE partition = $partition AND (d0 BETWEEN ? AND ?) AND ...;
	//where d0, d1, ... are "d{id}" for id in vp_ids
	std::unique_ptr<SQLite::Statement> ins_query;
	
	std::vector<int64_t> vp_ids_;
	
};