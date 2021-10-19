#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

class MVPTable
{
	//TODO: there's an annoying snag in the interface here, if this is ever to be generalized more
	//  specifically marshalling between blobs and the point's value
	//  it doesn't matter much in this application because they're both just vector<byte>
public:
	using blob_type = std::vector<uint8_t>;

	MVPTable();

	// Init with an open database
	// No transaction
	explicit MVPTable(std::shared_ptr<SQLite::Database> db);

	//Get the distance between two points -- to be implemented by subclass
	virtual int32_t distance(const blob_type& p1, const blob_type& p2) const = 0;

	// Insert a point into mvp_points, if it doesn't already exist, returning the id
	//  Each point is stored with the distances of the point to each vantage point
	//  And which partition it falls into
	// No transaction
	int64_t insert_point(const blob_type& p_value);

	// Insert a point into mvp_vantage_points, returning the new id
	// Throws a std::runtime_error if the point already exists
	// Adds a new "d{id}" column to mvp_points and fills it with the distance
	//   from each point to vp_vale
	// Recomputes the mvp_points part
	// No transaction
	int64_t insert_vantage_point(const blob_type& vp_value);

	// Get point ids close to q_value
	// 
	std::vector<int64_t> query(const blob_type& q_value, uint32_t radius, int64_t limit);

	blob_type find_vantage_point(size_t sample_size);

protected:
	using stmt_ptr = std::unique_ptr<SQLite::Statement>;

	//blob to vector
	static blob_type get_blob(SQLite::Column& col);

	stmt_ptr make_stmt(const std::string stmt);

	// Update cached vp_ids
	// Deletes ins_point if vp_ids has changed
	void update_vp_ids(const std::vector<int64_t>& vp_ids);

	// count all the rows of the given table (not cached)
	// No transaction
	int64_t exec_count_rows(const std::string& table);
	
	// get the cached row count of the given table
	// No transaction
	int64_t exec_sel_count(const std::string& table);

	// Increment the cached count of the given table
	// No transaction
	void exec_increment_count(const std::string& table);

	/* String constants */
	static constexpr const char str_init_tables[] = {
		//table sizes
		"CREATE TABLE IF NOT EXISTS mvp_counts ("
			"id INTEGER PRIMARY KEY,"
			"points INTEGER,"
			"vantange_points INTEGER,"
			"items INTEGER"
		");"

		"CREATE TABLE IF NOT EXISTS mvp_points ("
			"id INTEGER PRIMARY KEY,"
			//The partition is based on mvp_vantage_points bounds_0,1,2,3
			//  partition = sum( 4*id*(3 - shell(value, id)) for id in mvp_vantage_points)
			//  shell(value, id) is the index of which shell around the vantage point the value falls in
			"partition INTEGER,"
			"value BLOB UNIQUE"
		// "d0 INTEGER," etc are added later for each vantage_point with an ALTER TABLE
		");"
		"CREATE INDEX IF NOT EXISTS mvp_idx_points_part ON mvp_points(partition);"

		"CREATE TABLE IF NOT EXISTS mvp_vantage_points ("
			"id INTEGER PRIMARY KEY,"
			//bounds_0 is always 0
			"bounds_1 INTEGER DEFAULT 0," 
			"bounds_2 INTEGER DEFAULT 0,"
			"bounds_3 INTEGER DEFAULT 0,"
			"count_0 INTEGER DEFAULT 0," //number of points in shell 0 (0 <= d < bounds_1 )
			"count_1 INTEGER DEFAULT 0," //number of points in shell 1 (bounds_1 <= d < bounds_2)
			"count_2 INTEGER DEFAULT 0," //number of points in shell 2 (bounds_2 <= d < bounds_3)
			"count_3 INTEGER," //number of points in shell 3 (bounds_3 <= d )
			"value BLOB UNIQUE" // not necessarily in mvp_points
		");"

		"CREATE TABLE temp.mvp_query ("
			"id INTEGER PRIMARY KEY,"
			"dist INTEGER" //distance to query point
		");"
		"CREATE INDEX temp.mvp_idx_query_dist ON temp.mvp_query(dist);"
	};

	static constexpr const char str_count_rows[] =
		"SELECT COUNT(1) FROM $table;";

	static constexpr const char str_ins_counts[] =
		"INSERT INTO mvp_counts(points,vantage_points,items) VALUES($points,$vantage_points,$items)";

	static constexpr const char str_increment_count[] =
		"UPDATE mvp_counts SET $col = $col + 1 WHERE id = 1;";

	static constexpr const char str_sel_count[] =
		"SELECT $col FROM mvp_counts WHERE id = 1;";

	static constexpr const char str_sel_all_points[] =
		"SELECT id, value FROM mvp_points;";

	static constexpr const char str_sel_point_by_value[] =
		"SELECT id FROM mvp_points WHERE value = $value;";

	static constexpr const char str_upd_point[] =
		"UPDATE mvp_points SET $col = $value WHERE id = $id;";

	static constexpr const char str_add_points_col[] =
		"ALTER TABLE mvp_points ADD COLUMN $col INTEGER DEFAULT 0x7FFFFFFF;" // max int32
		"CREATE INDEX $idx ON mvp_points($col);";

	static constexpr const char str_sel_vp_ids[] =
		"SELECT id FROM mvp_vantage_points ORDER BY id ASC;";

	static constexpr const char str_sel_vps[] =
		"SELECT id, bounds_1, bounds_2, bounds_3, value FROM mvp_vantage_points ORDER BY id ASC;";

	static constexpr const char str_increment_vp_count[] =
		"UPDATE mvp_vantage_points SET $col = $col + 1 WHERE id = $id";

	static constexpr const char str_ins_vp[] =
		"INSERT INTO mvp_vantage_points(count_3,value) VALUES($count_3,$value) RETURNING id;";

	static constexpr const char str_del_query[] =
		"DELETE FROM temp.mvp_query;";

	static constexpr const char str_ins_query[] =
		"INSERT INTO temp.mvp_query(id, dist) VALUES($id, $dist);";

	//INSERT INTO mvp_points(part, value, d0, d1, ...) VALUES ($part, $value, $d0, $d1, ...) RETURNING id;
	//  where d0, d1, ... are "d{id}" for id in vp_ids
	static const std::string str_ins_point(const std::vector<int64_t>& vp_ids);

	//The database connection
	std::shared_ptr<SQLite::Database> db;

	/* Cached compiled SQL statememts */
	stmt_ptr count_rows; // $table: $table -> count
	stmt_ptr ins_counts; // mvp_counts: $points,$vantage_points,$parts,$items ->
	stmt_ptr increment_count; // mvp_counts: $col ->
	stmt_ptr sel_count; // mvp_counts: $col -> $col
	stmt_ptr sel_all_points; // mvp_points: -> id, value
	stmt_ptr sel_point_by_value; // mvp_points: $value -> id
	stmt_ptr ins_point; // mvp_points: $part, $value, $d0, ... -> id
	stmt_ptr upd_point; // mvp_points: $col, $value, $id ->
	stmt_ptr add_points_col; // mvp_points: $col, $idx ->
	stmt_ptr sel_vp_ids; // mvp_vantage_points: -> id
	stmt_ptr sel_vps; // mvp_vantage_points: -> id, bounds_1, bounds_2, bounds_3, value
	stmt_ptr increment_vp_count; // mvp_vantage_points: $col, $id -> 
	stmt_ptr ins_vp; // mvp_vantage_points: $value -> id
	stmt_ptr del_query; // mvp_query: ->
	stmt_ptr ins_query; // mvp_query: $id, $dist ->

	std::vector<int64_t> vp_ids_;
	
};