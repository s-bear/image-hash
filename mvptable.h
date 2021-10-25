#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <functional>

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

	blob_type find_vantage_point(size_t sample_size);

	void rebalance();

protected:

	void check_db();

	std::function<distance_fn> dist_fn_;

	// callback for "mvp_distance" sql function
	//   user data is this
	//   args are 2 point values, as blobs
	//   returns this->dist_fn_(args[0], args[1])
	static void sql_distance(sqlite3_context* ctx, int n, sqlite3_value* args[]);
	
	//blob to vector
	static blob_type get_blob(sqlite3_value* val);
	static blob_type get_blob(SQLite::Column& col);

	SQLite::Statement& get_stmt(const std::string& stmt);

	// Update cached vp_ids
	// Deletes ins_point if vp_ids has changed
	void update_vp_ids(const std::vector<int64_t>& vp_ids);

	// Increment the cached count of the given table
	// No transaction
	void exec_increment_count(const std::string& table);

	//INSERT INTO mvp_points(part, value, d0, d1, ...) VALUES ($part, $value, $d0, $d1, ...) RETURNING id;
	//where d0, d1, ... are "d{id}" for id in vp_ids
	static const std::string str_ins_point(const std::vector<int64_t>& vp_ids);

	//INSERT INTO temp.mvp_query(id, dist)
	//  SELECT id, mvp_distance($q_value, value) AS dist
	//    FROM mvp_points WHERE partition = $partition AND dist <= $radius;
	static const std::string str_ins_query(const std::vector<int64_t>& vp_ids);

	//The database connection
	std::shared_ptr<SQLite::Database> db;

	/* Cache for compiled SQL statememts */
	std::unordered_map<std::string, SQLite::Statement> stmt_cache_;
	// we treat these separately because they aren't static

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