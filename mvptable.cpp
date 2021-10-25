
#include "mvptable.h"
#include <sqlite3.h>
#include <algorithm>
#include <cassert>

namespace {
	
	//this bit of code happens often enough that it's worth factoring out
	inline SQLite::Column exec_get(SQLite::Statement& stmt, int col)
	{
		stmt.reset();
		if (stmt.executeStep()) {
			return stmt.getColumn(col);
		}
		else {
			throw std::runtime_error("Error executing statement");
		}
	}

	//each vantage point gets 2 bits of the partition, indexed by its id
	constexpr int64_t partition_offset(int64_t vp_id) { return 2 * vp_id; }
	constexpr int64_t partition_mask() { return 0x3; }

	constexpr int64_t partition_mask(int64_t vp_id) {
		return partition_mask() << partition_offset(vp_id);
	}
	constexpr int64_t partition_bits(int64_t shell, int64_t vp_id) {
		return shell << partition_offset(vp_id);
	}
}

//access cached statements
SQLite::Statement& MVPTable::get_stmt(const std::string& stmt)
{
	// look up stmt in the cache
	// if it doesn't exist yet, construct a new statement with (*db, stmt)
	// returns pair<iterator, bool> pointing to the item, and true if the new item was inserted
	auto res = stmt_cache_.try_emplace(stmt, *db, stmt);
	return res.first->second;
}

void MVPTable::check_db() {
	if (db == nullptr) throw std::runtime_error("No database connection");
}

MVPTable::MVPTable()
	: db(nullptr), dist_fn_()
{
	//nothing else to do
}

// Construct, open or create the database
MVPTable::MVPTable(std::shared_ptr<SQLite::Database> db, std::function<distance_fn> dist_fn) 
	: db(db), dist_fn_(dist_fn)
{
	if (db == nullptr) return;
	//initialize database as necessary
	db->exec(
		"CREATE TABLE IF NOT EXISTS mvp_counts ("
			"id INTEGER PRIMARY KEY,"
			"points INTEGER,"
			"vantange_points INTEGER"
		");"

		"CREATE TABLE IF NOT EXISTS mvp_points ("
			"id INTEGER PRIMARY KEY,"
			//The partition is based on mvp_vantage_points bound_0,1,2,3
			//  partition = sum( 4*id*shell(value, id)) for id in mvp_vantage_points)
			//  shell(value, id) is the index of which shell around the vantage point the value falls in
			"partition INTEGER,"
			//TODO: if the value blobs are big, it might be wise to add a hash for quick lookup?
			"value BLOB"
			// "d0 INTEGER," etc are added later for each vantage_point with an ALTER TABLE
		");"
		"CREATE INDEX IF NOT EXISTS mvp_idx_points_part ON mvp_points(partition);"
		"CREATE UNIQUE INDEX IF NOT EXISTS mvp_idx_points_value ON mvp_points(value);"

		"CREATE TABLE IF NOT EXISTS mvp_vantage_points ("
			"id INTEGER PRIMARY KEY,"
			//bound_0 is always 0
			"bound_1 INTEGER,"
			"bound_2 INTEGER,"
			"bound_3 INTEGER,"
			"count_0 INTEGER," //number of points in shell 0 (0 <= d < bound_1 )
			"count_1 INTEGER," //number of points in shell 1 (bound_1 <= d < bound_2)
			"count_2 INTEGER," //number of points in shell 2 (bound_2 <= d < bound_3)
			"count_3 INTEGER," //number of points in shell 3 (bound_3 <= d )
			"value BLOB UNIQUE" // not necessarily in mvp_points
		");"

		"CREATE TABLE temp.mvp_query ("
			"id INTEGER PRIMARY KEY,"
			"dist INTEGER" //distance to query point
		");"
		"CREATE INDEX temp.mvp_idx_query_dist ON temp.mvp_query(dist);"
	);

	db->createFunction("mvp_distance", 2, true, this, MVPTable::sql_distance);

	auto& count_rows = get_stmt("SELECT COUNT(1) FROM $table;");
	auto& ins_counts = get_stmt("INSERT INTO mvp_counts(points,vantage_points)"
									"VALUES($points,$vantage_points)");
	
	//if counts is empty, initialize it
	count_rows.bind("table", "mvp_counts");
	if (exec_get(count_rows,0).getInt64() == 0) {
		count_rows.bind("table", "mvp_points");
		auto num_points = exec_get(count_rows, 0).getInt64();

		count_rows.bind("table", "mvp_vantage_points");
		auto num_vantage_points = exec_get(count_rows, 0).getInt64();
		
		ins_counts.bind("points", num_points);
		ins_counts.bind("vantage_points", num_vantage_points);
		ins_counts.reset();
		ins_counts.exec();
	}

	auto& sel_vp_ids = get_stmt("SELECT id FROM mvp_vantage_points ORDER BY id ASC;");
	//get all of the vantage point ids
	std::vector<int64_t> vp_ids;
	sel_vp_ids.reset();
	while (sel_vp_ids.executeStep()) {
		vp_ids.push_back(sel_vp_ids.getColumn(0).getInt64());
	}
	update_vp_ids(vp_ids);
}

void MVPTable::sql_distance(sqlite3_context* ctx, int n, sqlite3_value* args[])
{
	auto mvp_table = static_cast<MVPTable*>(sqlite3_user_data(ctx));
	if (n != 2) {
		sqlite3_result_error(ctx, "mvp_distance requires 2 arguments.", -1);
		return;
	}
	auto p1 = get_blob(args[0]);
	auto p2 = get_blob(args[1]);
	auto d = mvp_table->dist_fn_(p1, p2);
	sqlite3_result_int(ctx, d);
}

const std::string MVPTable::str_ins_point(const std::vector<int64_t>& vp_ids) {
	std::string stmt1 = "INSERT INTO mvp_points(part, value";
	std::string stmt2 = ") VALUES ($part, $value";
	for (int64_t id : vp_ids) {
		auto id_str = std::to_string(id);
		stmt1 += ", d" + id_str;
		stmt2 += ", $d" + id_str;
	}
	return stmt1 + stmt2 + ") RETURNING id;";
}

const std::string MVPTable::str_ins_query(const std::vector<int64_t>& vp_ids)
{
	std::string stmt = 
		"INSERT INTO temp.mvp_query(id, dist) "
		"SELECT id, mvp_distance($q_value, value) AS dist "
		"FROM mvp_points WHERE partition = $partition AND dist <= $radius;";
	return stmt + ";";
}

MVPTable::blob_type MVPTable::get_blob(sqlite3_value* val)
{
	//TODO: error checking
	size_t n = sqlite3_value_bytes(val);
	const uint8_t* data = static_cast<const uint8_t*>(sqlite3_value_blob(val));
	return blob_type(data, data + n);
}

MVPTable::blob_type MVPTable::get_blob(SQLite::Column& col) 
{
	size_t n = col.getBytes();
	const uint8_t* data = static_cast<const uint8_t*>(col.getBlob());
	return blob_type(data, data + n);
}

void MVPTable::update_vp_ids(const std::vector<int64_t>& vp_ids)
{
	if (!std::equal(vp_ids_.begin(), vp_ids_.end(), vp_ids.begin(), vp_ids.end())) {
		ins_point = std::make_unique<SQLite::Statement>(*db, str_ins_point(vp_ids));
		ins_query = std::make_unique<SQLite::Statement>(*db, str_ins_query(vp_ids));
		vp_ids_ = vp_ids;
	}
}

void MVPTable::exec_increment_count(const std::string& table) {
	auto& stmt = get_stmt("UPDATE mvp_counts SET $table = $table + 1 WHERE id = 1;");
	stmt.bindNoCopy("table", table);
	stmt.reset();
	stmt.exec();
}

int64_t MVPTable::insert_point(const blob_type& p_value)
{
	check_db();

	//is the point already in the database?
	auto& sel_pt = get_stmt("SELECT id FROM mvp_points WHERE value = $value;");
	sel_pt.bindNoCopy("value", p_value.data(), p_value.size());
	sel_pt.reset();
	if (sel_pt.executeStep()) {
		//yes
		return sel_pt.getColumn(0).getInt64();
	}
	else {
		//no: we need to add the point

		//iterate over the vantage points
		//   calculating the distance from each to p_value (dist)
		//   and which shell p_value falls into

		auto& sel_vps = get_stmt(
			"SELECT"
				"id,"
				"mvp_distance(value, $pt) AS dist,"
				"CASE "
					"WHEN dist >= bound_3 THEN 3 "
					"WHEN dist >= bound_2 THEN 2 "
					"WHEN dist >= bound_1 THEN 1 "
					"ELSE 0 "
				"END AS shell "
			"FROM mvp_vantage_points ORDER BY id ASC;"
		);

		auto& inc_vp_count = get_stmt(
			"UPDATE mvp_vantage_points SET $count = $count + 1 WHERE id = $id"
		);

		std::vector<int64_t> vp_ids;
		std::vector<int32_t> dists;
		int64_t part = 0;
		sel_vps.bindNoCopy("pt", p_value.data(), static_cast<int>(p_value.size()));
		sel_vps.reset();
		while (sel_vps.executeStep()) {
			auto id = sel_vps.getColumn("id").getInt64();
			auto dist = sel_vps.getColumn("dist").getInt();
			auto shell = sel_vps.getColumn("shell").getInt();

			vp_ids.push_back(id);
			dists.push_back(dist);

			//which shell around the vantage point does this point fall into
			// also prepare the increment_vp_count statement for incrementing the occupancy count of that shell
			inc_vp_count.bind("id", id);
			if (shell == 3) inc_vp_count.bind("count", "count_3");
			else if (shell == 2) inc_vp_count.bind("count", "count_2");
			else if (shell == 1) inc_vp_count.bind("count", "count_1");
			else if (shell == 0) inc_vp_count.bind("count", "count_0");
			else {
				throw std::runtime_error("Error inserting point: invalid shell");
			}
			inc_vp_count.reset();
			inc_vp_count.exec();

			//calculate the partition
			part |= partition_bits(shell, id);
		}

		//update the insert_point statement if vp_ids changed
		update_vp_ids(vp_ids);

		ins_point->bind("part", part);
		ins_point->bindNoCopy("value", p_value.data(), p_value.size());
		for (size_t i = 0; i < dists.size(); ++i) {
			ins_point->bind(i + 3, dists[i]); //the first parameter has index 1, so these start at 3
		}
		ins_point->reset();
		if (ins_point->executeStep()) {
			exec_increment_count("points");

			return ins_point->getColumn(0).getInt64();
		}
		else {
			throw std::runtime_error("Error inserting point");
		}
	}
}

int64_t MVPTable::insert_vantage_point(const blob_type& vp_value)
{
	check_db();
	//TODO: maximum number of vantage points? at 4 shells per, the number of partitions hits 64 bits at 32

	// Inserting a vantage point:
	//   adds a new row to mvp_vantage_points
	//   adds a new column of distances to mvp_points
	//   calculate the distance from the vantage point to each point
	//   balance the partitions of the new vantage point by percentiles
	
	// calculating the distances and balancing the partitions both touch
	// all of the points, but this isn't necessary. We could subsample
	// the points (e.g. take n random points from each existing partition)
	// and calculate the partition bounds from them instead of using the whole dataset

	//1. add the new row to mvp_vantage_points and get the new vp's id
	
	auto& ins_vp = get_stmt(
		"INSERT INTO mvp_vantage_points(value) VALUES($value) RETURNING id;"
	);

	int64_t vp_id;
	ins_vp.bindNoCopy("value", vp_value.data(), vp_value.size());
	ins_vp.reset();
	if (ins_vp.executeStep()) {
		vp_id = ins_vp.getColumn("id").getInt64();
		exec_increment_count("vantage_points");
	}
	else {
		throw std::runtime_error("Error inserting new vantage_point");
	}

	//2. add the new column of distances to mvp_points
	std::string col_name = "d" + std::to_string(vp_id);
	auto& add_col = get_stmt(
		"ALTER TABLE mvp_points ADD COLUMN $col INTEGER;"
		"UPDATE mvp_points SET $col = mvp_distance($vp_value, value);"
	);
	add_col.bind("col", col_name);
	add_col.bindNoCopy("vp_value", vp_value.data(), vp_value.size());
	add_col.reset();
	add_col.exec();

	//3. choose the shell boundaries so that they're balanced
	//   we want to find the 25%, 50%, and 75% ranked distances
	auto& sel_points_count = get_stmt("SELECT points FROM mvp_counts WHERE id = 1;");
	int64_t point_count = exec_get(sel_points_count, 0).getInt64();
	int64_t rank_25 = point_count / 4;
	int64_t rank_50 = point_count / 2;
	int64_t rank_75 = rank_50 + rank_25;
	auto& find_bound = get_stmt(
		"SELECT $col FROM mvp_points ORDER BY $col LIMIT 1 OFFSET $rank;"
	);
	find_bound.bind("col", col_name);
	find_bound.bind("rank", rank_25);
	auto bound_1 = exec_get(find_bound,0).getInt();
	find_bound.bind("rank", rank_50);
	auto bound_2 = exec_get(find_bound,0).getInt();
	find_bound.bind("rank", rank_75);
	auto bound_3 = exec_get(find_bound,0).getInt();

	// the shells include the lower bound, but exclude the upper
	// so the number of points in shell 0 is one less than the rank
	int64_t count_0 = rank_25 - 1;
	int64_t count_1 = rank_50 - rank_25;
	int64_t count_2 = rank_75 - rank_50;
	int64_t count_3 = point_count - rank_75;

	auto& upd_vp = get_stmt(
		"UPDATE mvp_vantage_points SET "
		"bound_1 = $b1, bound_2 = $b2, bound_3 = $b3,"
		"count_0 = $c0, count_1 = $c1, count_2 = $c2, count_3 = $c3 "
		"WHERE id = $id;"
	);
	upd_vp.bind("b1", bound_1);
	upd_vp.bind("b2", bound_2);
	upd_vp.bind("b3", bound_3);
	upd_vp.bind("c0", count_0);
	upd_vp.bind("c1", count_1);
	upd_vp.bind("c2", count_2);
	upd_vp.bind("c3", count_3);
	upd_vp.reset();
	upd_vp.exec();

	//4. Iterate over all of the points and update their partition for the new vantage point
	auto& upd_points_part = get_stmt(
		"UPDATE mvp_points SET "
		"partition = partition | ("
			"CASE " // as partition_bits()
				"WHEN $col >= $b3 THEN 3 "
				"WHEN $col >= $b2 THEN 2 "
				"WHEN $col >= $b1 THEN 1 "
				"ELSE 0 "
			"END << (2 * $vp_id));"
	);

	upd_points_part.bind("b1", bound_1);
	upd_points_part.bind("b2", bound_2);
	upd_points_part.bind("b3", bound_3);
	upd_points_part.bind("col", col_name);
	upd_points_part.bind("vp_id", vp_id);
	upd_points_part.reset();
	upd_points_part.exec();
}

int64_t MVPTable::query(const blob_type& q_value, uint32_t radius)
{
	check_db();

	//Iterate over all of the vantage points
	// getting the distance from each to the query point
	// and for each shell, whether that shell intersects the query ball
	auto& sel_vps = get_stmt(
		"SELECT "
			"id,"
			"mvp_distance(value, $pt) as dist,"
			"CASE WHEN dist + $rad >= bound_3 THEN 1 ELSE 0 END AS shell_3,"
			"CASE WHEN dist + $rad >= bound_2 AND dist - $rad < bound_3 THEN 1 ELSE 0 END AS shell_2,"
			"CASE WHEN dist + $rad >= bound_1 AND dist - $rad < bound_2 THEN 1 ELSE 0 END AS shell_1,"
			"CASE WHEN dist - $rad < bound_1 THEN 1 ELSE 0 END AS shell_0,"
		"FROM mvp_vantage_point ORDER BY id ASC;"
	);

	std::vector<int64_t> vp_ids;
	std::vector<int64_t> parts;
	parts.push_back(0); // which paritions the query ball covers
	sel_vps.bindNoCopy("pt", q_value.data(), static_cast<int>(q_value.size()));
	sel_vps.bind("rad", radius);
	sel_vps.reset();
	while (sel_vps.executeStep()) {
		auto id = sel_vps.getColumn("id").getInt64();
		auto dist = sel_vps.getColumn("dist").getInt();
		auto shell_3 = sel_vps.getColumn("shell_3").getInt();
		auto shell_2 = sel_vps.getColumn("shell_2").getInt();
		auto shell_1 = sel_vps.getColumn("shell_1").getInt();
		auto shell_0 = sel_vps.getColumn("shell_0").getInt();
		
		vp_ids.push_back(id);

		//TODO: I have no idea how this might be done in SQL
		std::vector<int> shells;
		if (shell_3 != 0) shells.push_back(3);
		if (shell_2 != 0) shells.push_back(2);
		if (shell_1 != 0) shells.push_back(1);
		if (shell_0 != 0) shells.push_back(0);
		if(shells.empty()) {
			throw std::runtime_error("Error querying point: invalid shells");
		}

		if (shells.size() == 1) {
			//a single shell -- we can modify the existing partitions in place
			for (auto& p : parts) {
				p |= partition_bits(shells[0], id);
			}
		}
		else {
			//multple shells -- the number of partitions the query covers will grow
			std::vector<int64_t> new_parts;
			new_parts.reserve(parts.size() * shells.size());
			for (auto p : parts) {
				for (auto s : shells) {
					new_parts.push_back(p | partition_bits(s, id));
				}
			}
			parts = std::move(new_parts);
		}
	}
	
	update_vp_ids(vp_ids);
	
	//populate the query table with the points covered by the partitions
	// sort by the distance to the query point

	//first clear the query table
	auto& clear_query = get_stmt("DELETE FROM temp.mvp_query;");
	clear_query.reset();
	clear_query.exec();
	
	// build the query 
	ins_query->bindNoCopy("q_value", q_value.data(), q_value.size());
	ins_query->bind("radius", radius);
	//run the query for each partition that the radius covers
	int64_t result_count = 0;
	for (auto p : parts) {
		ins_query->bind("partition", p);
		ins_query->reset();
		result_count += ins_query->exec();
	}
	return result_count;
}

MVPTable::blob_type MVPTable::find_vantage_point(size_t sample_size)
{
	check_db();
	
	//do we have any points yet?
	auto& pt_count = get_stmt("SELECT points FROM mvp_counts WHERE id = 1;");
	auto num_points = exec_get(pt_count, 0).getInt64();
	if (num_points <= 0) {
		throw std::runtime_error("Empty table");
	}

	// do we have any vantage points yet?
	auto& vp_count = get_stmt("SELECT vantage_points FROM mvp_counts WHERE id = 1;");
	auto num_vantage_points = exec_get(vp_count, 0).getInt64();

	std::string stmt;
	bool do_bind = false; //do we need to bind sample_size?
	if (num_vantage_points > 0) {
		//we need to find a point that's far from all of the existing vantage points
		// the partition index increases with distance from each vantage point, so
		// we just need to pick a point in the maximum partition
		// NB this weights the importance of the vantage points by newest to oldest
		stmt = "SELECT value FROM mvp_points ORDER BY partition DESC, random() LIMIT 1;";
	}
	else {
		//we need to find a point that's far from most other points
		// since we don't have any vantage points yet, we will rank
		// the points by the sum of the distances to all of the other points
		// and pick the greatest
		if (num_points <= sample_size) {
			//we have few points, so do the pairwise distance between all
			stmt = "SELECT value FROM ("
				"SELECT p.value AS value, sum(mvp_distance(p.value, q.value)) AS sum_dist"
				"FROM mvp_points p, mvp_points q GROUP BY p.id"
				") ORDER BY sum_dist DESC LIMIT 1;";
		}
		else {
			//subsample the points, then do the pairwise distance between them
			// NB this method of sampling is not very efficient -- it still touches all of the points
			// TODO: more efficient random sampling?
			stmt = "WITH sampled_points AS (SELECT value FROM mvp_points ORDER BY random() LIMIT $sample_size) "
				"SELECT value FROM ("
				"SELECT p.value AS value, sum(mvp_distance(p.value, q.value)) AS sum_dist"
				"FROM sampled_points p, sampled_points q GROUP BY p.id"
				") ORDER BY sum_dist DESC LIMIT 1;";
			do_bind = true; //we need to bind sample_size
		}
	}
	auto& find_far_point = get_stmt(stmt);
	if (do_bind) find_far_point.bind("sample_size", static_cast<int64_t>(sample_size));
	return get_blob(exec_get(find_far_point, 0));
}

void MVPTable::rebalance()
{
	//TODO
	throw std::runtime_error("Not implemented");
}