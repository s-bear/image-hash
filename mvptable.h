#pragma once

#include "sqlitecpp.h"
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <functional>

class MVPTable
{
public:
	using blob_view = SQLite::blob_view;
	using distance_fn = int(blob_view, blob_view);

	static void register_distance_function(SQLite::Database& db, distance_fn& fn);

	// Init with an open database
	// No transaction
	MVPTable(SQLite::Database& db);

	// Insert a point into mvp_points if it doesn't already exist
	//  Each point is stored with the distances of the point to each vantage point
	//  And which partition it falls into
	// No transaction
	// Returns the id of the point
	sqlite_int64 insert_point(blob_view p_value);

	//how many points are there (cached)?
	sqlite_int64 count_points();
	sqlite_int64 count_vantage_points();

	// Insert a point into mvp_vantage_points
	// Throws a std::runtime_error if the point already exists
	// Adds a new "d{id}" column to mvp_points and fills it with the distance
	//   from each point to vp_value
	// Recomputes the mvp_points partition, balancing only the new vantage point
	// No transaction
	// Returns the id of the vantage point
	sqlite_int64 insert_vantage_point(blob_view vp_value);

	// Get point ids within `radius` of `q_value`
	// The results (id, dist) are stored in the temp.mvp_query table
	// Returns the number of points found
	sqlite_int64 query(blob_view q_value, uint32_t radius);

	// Find a point that would make a good vantage point
	blob_view find_vantage_point(size_t sample_size);

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
	static std::function<distance_fn> s_distance_fn;


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
	


	//INSERT INTO mvp_points(part, value, d0, d1, ...) VALUES ($part, $value, $d0, $d1, ...) RETURNING id;
	//where d0, d1, ... are "d{id}" for id in vp_ids
	SQLite::Statement make_insert_point(const std::vector<sqlite_int64>& vp_ids);

	//INSERT INTO temp.mvp_query(id, dist)
	//  SELECT id, mvp_distance($q_value, value) AS dist
	//    FROM mvp_points WHERE partition = $partition AND dist <= $radius;
	static const std::string str_ins_query(const std::vector<int64_t>& vp_ids);

	//The database connection
	SQLite::Database& m_db;
	
	//Cached queries
	SQLite::Statement m_count_points, m_count_vantage_points, m_add_point, m_add_vantage_point;

	SQLite::Statement m_find_point, m_insert_point, m_select_vps;
	SQLite::Statement m_vp_count_0, m_vp_count_1, m_vp_count_2, m_vp_count_3;

	SQLite::Statement m_insert_vantage_point;

	// insert_point() statements
	SQLite::Statement m_ip_find_pt, m_ip_sel_vps, m_ip_insert_pt;
	

	// query() statements
	SQLite::Statement m_q_sel_vps;

	//INSERT INTO temp.mvp_query(id, dist)
	//  SELECT id, mvp_distance($q_value, value) FROM mvp_points
	//    WHERE partition = $partition AND (d0 BETWEEN ? AND ?) AND ...;
	//where d0, d1, ... are "d{id}" for id in vp_ids
	SQLite::Statement m_insert_query;
};