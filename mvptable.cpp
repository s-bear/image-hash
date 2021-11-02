
#include "mvptable.h"
#include <sqlite3.h>
#include <algorithm>
#include <cassert>

SQLStatementCache::SQLStatementCache() : db(nullptr)
{
	//nothing to do
}

SQLStatementCache::SQLStatementCache(std::shared_ptr<SQLite::Database> db) : db(db)
{
	//nothing to do
}

//access cached statements
SQLite::Statement& SQLStatementCache::get(const std::string& stmt)
{
	// look up stmt in the cache
	// if it doesn't exist yet, construct a new statement with (*db, stmt)
	// returns pair<iterator, bool> pointing to the item, and true if the new item was inserted
	if (!db) throw std::runtime_error("No database connection");
	auto res = stmts.try_emplace(stmt, *db, stmt);
	return res.first->second;
}

void SQLStatementCache::exec(SQLite::Statement& stmt) 
{
	stmt.reset();
	stmt.exec();
}

SQLite::Column SQLStatementCache::exec(SQLite::Statement& stmt, int col)
{
	stmt.reset();
	if (stmt.executeStep()) {
		return stmt.getColumn(col);
	}
	else {
		throw std::runtime_error("Error executing statement");
	}
}

SQLite::Column SQLStatementCache::exec(SQLite::Statement& stmt, const std::string& col)
{
	stmt.reset();
	if (stmt.executeStep()) {
		return stmt.getColumn(col.c_str());
	}
	else {
		throw std::runtime_error("Error executing statement");
	}
}

void SQLStatementCache::exec(const std::string& stmt)
{
	exec(get(stmt));
}

SQLite::Column SQLStatementCache::exec(const std::string& stmt, int col)
{
	return exec(get(stmt), col);
}

SQLite::Column SQLStatementCache::exec(const std::string& stmt, const std::string& col)
{
	return exec(get(stmt), col);
}

void MVPTable::check_db() {
	if (db == nullptr) throw std::runtime_error("No database connection");
}

MVPTable::MVPTable()
	: db(nullptr), cache(nullptr)
{
	//nothing else to do
}

// Construct, open or create the database
MVPTable::MVPTable(std::shared_ptr<SQLite::Database> db, std::function<distance_fn> dist_fn) 
	: db(db), cache(db)
{
	if (db == nullptr) return;
	
	//initialize database as necessary
	db->exec(
		"CREATE TABLE IF NOT EXISTS mvp_counts ("
		"id INTEGER PRIMARY KEY,"
		"points INTEGER,"
		"vantage_points INTEGER"
		");"
	);
	db->exec(
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
	);
	db->exec(
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
	);
	db->exec(
		"CREATE TEMPORARY TABLE mvp_query ("
			"id INTEGER PRIMARY KEY,"
			"dist INTEGER" //distance to query point
		");"
	);
	
	//if counts is empty, initialize it
	if (db->execAndGet("SELECT COUNT(1) FROM mvp_counts").getInt64() == 0) {
		auto num_points = db->execAndGet("SELECT COUNT(1) FROM mvp_points").getInt64();

		auto num_vantage_points = db->execAndGet("SELECT COUNT(1) FROM mvp_vantage_points").getInt64();

		auto ins_counts = SQLite::Statement(*db,
			"INSERT INTO mvp_counts(points,vantage_points)"
			"VALUES($points,$vantage_points);");

		ins_counts.bind("$points", num_points);
		ins_counts.bind("$vantage_points", num_vantage_points);
		ins_counts.exec();
	}
	
	set_dist_fn(std::move(dist_fn));
	db->createFunction("mvp_distance", 2, true, nullptr, MVPTable::sql_distance);
}

std::function<MVPTable::distance_fn> MVPTable::dist_fn;

void MVPTable::set_dist_fn(std::function<distance_fn> df)
{
	dist_fn = std::move(df);
}

void MVPTable::sql_distance(sqlite3_context* ctx, int n, sqlite3_value* args[])
{
	if (n != 2) {
		sqlite3_result_error(ctx, "mvp_distance requires 2 arguments.", -1);
		return;
	}
	auto p1 = get_blob(args[0]);
	auto p2 = get_blob(args[1]);
	auto d = dist_fn(p1, p2);
	sqlite3_result_int(ctx, d);
}

const std::string MVPTable::str_ins_point(const std::vector<int64_t>& vp_ids) {
	std::string stmt1 = "INSERT INTO mvp_points(partition, value";
	std::string stmt2 = ") VALUES ($partition, $value";
	for (int64_t id : vp_ids) {
		auto id_str = std::to_string(id);
		stmt1 += ", d" + id_str;
		stmt2 += ", $d" + id_str;
	}
	return stmt1 + stmt2 + ") RETURNING id;";
}

const std::string MVPTable::str_ins_query(const std::vector<int64_t>& vp_ids)
{
	return std::string(
		"INSERT INTO mvp_query(id, dist) "
		"SELECT id, mvp_distance($q_value, value) AS dist "
		"FROM mvp_points WHERE partition = $partition AND dist <= $radius;"
	);
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

int64_t MVPTable::count_points() {
	return cache.exec("SELECT points FROM mvp_counts WHERE id = 1;", 0).getInt64();
}

int64_t MVPTable::count_vantage_points() {
	return cache.exec("SELECT vantage_points FROM mvp_counts WHERE id = 1;", 0).getInt64();
}

int64_t MVPTable::insert_point(const blob_type& p_value)
{
 	check_db();

	//is the point already in the database?
	auto& sel_pt = cache["SELECT id FROM mvp_points WHERE value = $value;"];
	sel_pt.reset();
	sel_pt.bind("$value", p_value.data(), static_cast<int>(p_value.size()));
	if (sel_pt.executeStep()) {
		//yes
		auto id = sel_pt.getColumn(0).getInt64();
		return id;
	}
	else {
		//no: we need to add the point

		//iterate over the vantage points
		//   calculating the distance from each to p_value (dist)
		//   and which shell p_value falls into

		auto& sel_vps = cache[
			"WITH vp_dists AS ("
				"SELECT id, mvp_distance(value, $pt) AS dist "
				"FROM mvp_vantage_points ORDER BY id ASC) "
			"SELECT id, dist, CASE "
					"WHEN dist >= bound_3 THEN 3 "
					"WHEN dist >= bound_2 THEN 2 "
					"WHEN dist >= bound_1 THEN 1 "
					"ELSE 0 "
				"END AS shell "
			"FROM vp_dists JOIN mvp_vantage_points USING (id) ORDER BY id ASC;"
		];

		auto& inc_count_0 = cache["UPDATE mvp_vantage_points SET count_0 = count_0 + 1 WHERE id = $id"];
		auto& inc_count_1 = cache["UPDATE mvp_vantage_points SET count_1 = count_1 + 1 WHERE id = $id"];
		auto& inc_count_2 = cache["UPDATE mvp_vantage_points SET count_2 = count_2 + 1 WHERE id = $id"];
		auto& inc_count_3 = cache["UPDATE mvp_vantage_points SET count_3 = count_3 + 1 WHERE id = $id"];

		std::vector<int64_t> vp_ids;
		std::vector<int32_t> dists;
		int64_t part = 0;
		sel_vps.reset();
		sel_vps.bind("$pt", p_value.data(), static_cast<int>(p_value.size()));
		while (sel_vps.executeStep()) {
			auto id = sel_vps.getColumn("id").getInt64();
			auto dist = sel_vps.getColumn("dist").getInt();
			auto shell = sel_vps.getColumn("shell").getInt();

			vp_ids.push_back(id);
			dists.push_back(dist);

			//increment shell count
			if (shell == 3) {
				inc_count_3.reset();
				inc_count_3.bind("$id", id);
				inc_count_3.exec();
			}
			else if (shell == 2) {
				inc_count_2.reset();
				inc_count_2.bind("$id", id);
				inc_count_2.exec();
			}
			else if (shell == 1) {
				inc_count_1.reset();
				inc_count_1.bind("$id", id);
				inc_count_1.exec();
			}
			else if (shell == 0) {
				inc_count_0.reset();
				inc_count_0.bind("$id", id);
				inc_count_0.exec();
			}
			else {
				throw std::runtime_error("Error inserting point: invalid shell");
			}

			//calculate the partition
			part |= partition_bits(shell, id);
		}

		//update the insert_point statement if vp_ids changed
		update_vp_ids(vp_ids);

		ins_point->reset();
		ins_point->bind("$partition", part);
		ins_point->bind("$value", p_value.data(), static_cast<int>(p_value.size()));
		for (int i = 0; i < dists.size(); ++i) {
			ins_point->bind(i + 3, dists[i]); //the first parameter has index 1, so these start at 3
		}
		if (ins_point->executeStep()) {
			cache.exec("UPDATE mvp_counts SET points = points + 1 WHERE id = 1;");
			
			auto id = ins_point->getColumn(0).getInt64();
			return id;
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
	
	auto& ins_vp = cache[
		"INSERT INTO mvp_vantage_points(value) VALUES($value) RETURNING id;"
	];

	int64_t vp_id;
	ins_vp.reset();
	ins_vp.bind("$value", vp_value.data(), static_cast<int>(vp_value.size()));
	if (ins_vp.executeStep()) {
		vp_id = ins_vp.getColumn("id").getInt64();
		cache.exec("UPDATE mvp_counts SET vantage_points = vantage_points + 1 WHERE id = 1;");
	}
	else {
		throw std::runtime_error("Error inserting new vantage_point");
	}

	//2. add the new column of distances to mvp_points
	std::string col_name = "d" + std::to_string(vp_id);

	db->exec("ALTER TABLE mvp_points ADD COLUMN " + col_name + " INTEGER;");
	
	db->exec("CREATE INDEX mvp_idx_" + col_name + " ON mvp_points(" + col_name + ");");
	// we have to commit now, to add the column
	
	auto upd_col = SQLite::Statement(*db, "UPDATE mvp_points SET " + col_name + " = mvp_distance($vp_value, value);");
	
	//3. choose the shell boundaries so that they're balanced
	//   we want to find the 25%, 50%, and 75% ranked distances
	auto point_count = count_points();
	int32_t bound_1, bound_2, bound_3;
	int64_t count_0, count_1, count_2, count_3;
	if (point_count > 50) {
		auto rank_25 = point_count / 4;
		auto rank_50 = point_count / 2;
		auto rank_75 = rank_50 + rank_25;
		auto find_bound = SQLite::Statement(*db,
			"SELECT " + col_name + " FROM mvp_points ORDER BY " + col_name + " LIMIT 1 OFFSET $rank;"
		);
		find_bound.bind("$rank", rank_25);
		bound_1 = cache.exec(find_bound, 0).getInt();
		find_bound.bind("$rank", rank_50);
		bound_2 = cache.exec(find_bound, 0).getInt();
		find_bound.bind("$rank", rank_75);
		bound_3 = cache.exec(find_bound, 0).getInt();

		// the shells include the lower bound, but exclude the upper
		// so the number of points in shell 0 is one less than the rank
		count_0 = rank_25 - 1;
		count_1 = rank_50 - rank_25;
		count_2 = rank_75 - rank_50;
		count_3 = point_count - rank_75;
	}
	else {
		bound_1 = bound_2 = bound_3 = 0;
		count_0 = count_1 = count_2 = 0;
		count_3 = point_count;
	}
	
	auto& upd_vp = cache[
		"UPDATE mvp_vantage_points SET "
		"bound_1 = $b1, bound_2 = $b2, bound_3 = $b3,"
		"count_0 = $c0, count_1 = $c1, count_2 = $c2, count_3 = $c3 "
		"WHERE id = $id;"
	];
	upd_vp.reset();
	upd_vp.bind("$id", vp_id);
	upd_vp.bind("$b1", bound_1);
	upd_vp.bind("$b2", bound_2);
	upd_vp.bind("$b3", bound_3);
	upd_vp.bind("$c0", count_0);
	upd_vp.bind("$c1", count_1);
	upd_vp.bind("$c2", count_2);
	upd_vp.bind("$c3", count_3);
	upd_vp.exec();

	//4. Iterate over all of the points and update their partition for the new vantage point
	auto upd_points_part = SQLite::Statement(*db,
		"UPDATE mvp_points SET "
		"partition = partition | ("
		"CASE " // as partition_bits()
		"WHEN " + col_name + " >= $b3 THEN 3 "
		"WHEN " + col_name + " >= $b2 THEN 2 "
		"WHEN " + col_name + " >= $b1 THEN 1 "
		"ELSE 0 "
		"END << (2 * $vp_id));"
	);

	upd_points_part.reset();
	upd_points_part.bind("$b1", bound_1);
	upd_points_part.bind("$b2", bound_2);
	upd_points_part.bind("$b3", bound_3);
	upd_points_part.bind("$vp_id", vp_id);
	upd_points_part.exec();

	return vp_id;
}

int64_t MVPTable::query(const blob_type& q_value, uint32_t radius)
{
	check_db();

	//Iterate over all of the vantage points
	// getting the distance from each to the query point
	// and for each shell, whether that shell intersects the query ball
	auto& sel_vps = cache[
		"WITH vp_dists AS ("
			"SELECT id, mvp_distance(value, $pt) AS dist "
			"FROM mvp_vantage_points ORDER BY id ASC) "
		"SELECT "
			"id,"
			"dist,"
			"CASE WHEN dist + $rad >= bound_3 THEN 1 ELSE 0 END AS shell_3,"
			"CASE WHEN bound_3 > bound_2 AND dist + $rad >= bound_2 AND dist - $rad < bound_3 THEN 1 ELSE 0 END AS shell_2,"
			"CASE WHEN bound_2 > bound_1 AND dist + $rad >= bound_1 AND dist - $rad < bound_2 THEN 1 ELSE 0 END AS shell_1,"
			"CASE WHEN bound_1 > 0 AND dist - $rad < bound_1 THEN 1 ELSE 0 END AS shell_0 "
		"FROM vp_dists JOIN mvp_vantage_points USING (id) ORDER BY id ASC;"
	];

	std::vector<int64_t> vp_ids;
	std::vector<int64_t> parts;
	parts.push_back(0); // which paritions the query ball covers
	sel_vps.reset();
	sel_vps.bind("$pt", q_value.data(), static_cast<int>(q_value.size()));
	sel_vps.bind("$rad", radius);
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
	auto& clear_query = cache["DELETE FROM mvp_query;"];
	clear_query.reset();
	clear_query.exec();
	
	// build the query 
	ins_query->reset();
	ins_query->bind("$q_value", q_value.data(), static_cast<int>(q_value.size()));
	ins_query->bind("$radius", radius);
	//run the query for each partition that the radius covers
	int64_t result_count = 0;
	for (auto p : parts) {
		ins_query->bind("$partition", p);
		result_count += ins_query->exec();
		ins_query->reset();
	}
	return result_count;
}

MVPTable::blob_type MVPTable::find_vantage_point(size_t sample_size)
{
	check_db();
	
	//do we have any points yet?
	auto num_points = count_points();
	if (num_points <= 0) {
		throw std::runtime_error("Empty table");
	}

	// do we have any vantage points yet?
	auto num_vantage_points = count_vantage_points();

	if (num_vantage_points > 0) {
		//we need to find a point that's far from all of the existing vantage points
		// the partition index increases with distance from each vantage point, so
		// we just need to pick a point in the maximum partition
		// NB this weights the importance of the vantage points by newest to oldest
		return get_blob(cache.exec(
			"SELECT value FROM mvp_points ORDER BY partition DESC, random() LIMIT 1;", 0));
	}
	else {
		//we need to find a point that's far from most other points
		// since we don't have any vantage points yet, we will rank
		// the points by the sum of the distances to all of the other points
		// and pick the greatest
		if (num_points <= sample_size) {
			//we have few points, so do the pairwise distance between all
			return get_blob(cache.exec(
				"SELECT value FROM ("
				"SELECT p.value AS value, sum(mvp_distance(p.value, q.value)) AS sum_dist"
				"FROM mvp_points p, mvp_points q GROUP BY p.id"
				") ORDER BY sum_dist DESC LIMIT 1;", 0));
		}
		else {
			//subsample the points, then do the pairwise distance between them
			// NB this method of sampling is not very efficient -- it still touches all of the points
			// TODO: more efficient random sampling?
			auto& stmt = cache["WITH sampled_points AS (SELECT value FROM mvp_points ORDER BY random() LIMIT $sample_size) "
				"SELECT value FROM ("
				"SELECT p.value AS value, sum(mvp_distance(p.value, q.value)) AS sum_dist"
				"FROM sampled_points p, sampled_points q GROUP BY p.id"
				") ORDER BY sum_dist DESC LIMIT 1;"];
			stmt.bind("$sample_size", static_cast<int64_t>(sample_size));
			return get_blob(cache.exec(stmt, 0));
		}
	}
}

void MVPTable::rebalance()
{
	//TODO
	throw std::runtime_error("Not implemented");
}