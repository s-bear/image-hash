
#include "mvptable.h"
#include <sqlite3.h>
#include <cmath>
#include <algorithm>
#include <cassert>

/* MVPTable static members*/
std::function<MVPTable::distance_fn> MVPTable::s_distance_fn;

void MVPTable::register_distance_function(SQLite::Database& db, distance_fn& fn)
{
	s_distance_fn = fn;
	db.create_function("mvp_distance", fn);
}

/* non-static */

void MVPTable::check_db() {
	if (!m_db) throw std::runtime_error("No database connection");
}

// Construct, open or create the database
MVPTable::MVPTable(SQLite::Database& db) 
	: m_db(db)
{
	//initialize database as necessary
	m_db.exec(
		"CREATE TABLE IF NOT EXISTS mvp_counts ("
		"id INTEGER PRIMARY KEY,"
		"points INTEGER,"
		"vantage_points INTEGER"
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
	
		"CREATE TEMPORARY TABLE mvp_query ("
			"id INTEGER PRIMARY KEY,"
			"dist INTEGER" //distance to query point
		");"
	);
	
	//if counts is empty, initialize it
	if (m_db("SELECT COUNT(1) FROM mvp_counts").step_get_int64() == 0) {
		auto num_points = m_db("SELECT COUNT(1) FROM mvp_points").step_get_int64();

		auto num_vantage_points = db("SELECT COUNT(1) FROM mvp_vantage_points").step_get_int64();

		auto ins_counts = db(
			"INSERT INTO mvp_counts(points,vantage_points)"
			"VALUES($points,$vantage_points);");

		ins_counts.bind("$points", num_points);
		ins_counts.bind("$vantage_points", num_vantage_points);
		ins_counts.exec();
	}
}

SQLite::Statement MVPTable::make_insert_point(const std::vector<int64_t>& vp_ids) {
	static const std::string_view s1 = "INSERT INTO mvp_points(partition, value";
	static const std::string_view s2 = ", d"; // repeated for each vp_id
	static const std::string_view s3 = ") VALUES ($partition, $value";
	static const std::string_view s4 = ", $d"; // repeated for each vp_id
	static const std::string_view s5 = ") RETURNING id;";

	size_t n = vp_ids.size();
	std::vector<std::string> id_strs;
	id_strs.reserve(n);
	size_t ids_len = 0;
	for (auto id : vp_ids) {
		auto id_str = std::to_string(id);
		ids_len += id_str.size();
		id_strs.push_back(std::move(id_str));
	}

	std::string stmt(s1);
	stmt.reserve(s1.size() + (s2.size() + s4.size())*n + ids_len*2 + s3.size() + s5.size());
	for (auto& id : id_strs) stmt.append(s2).append(id);
	stmt.append(s3);
	for (auto& id : id_strs) stmt.append(s4).append(id);
	stmt.append(s5);
	return m_db(stmt);
}

const std::string MVPTable::str_ins_query(const std::vector<int64_t>& vp_ids)
{
	return std::string(
		"INSERT INTO mvp_query(id, dist) "
		"SELECT id, mvp_distance($q_value, value) AS dist "
		"FROM mvp_points WHERE partition = $partition AND dist <= $radius;"
	);
}

int64_t MVPTable::count_points() {
	if (!m_count_points) m_count_points = m_db("SELECT points FROM mvp_counts WHERE id = 1;");
	return m_count_points.reset().step_get_int64();
}

int64_t MVPTable::count_vantage_points() {
	if (!m_count_vantage_points) m_count_vantage_points = m_db("SELECT vantage_points FROM mvp_counts WHERE id = 1;");
	return m_count_vantage_points.reset().step_get_int64();
}

int64_t MVPTable::insert_point(blob_view p_value)
{
 	check_db();

	//is the point already in the database?
	if (!m_find_point) m_find_point = m_db("SELECT id FROM mvp_points WHERE value = $value;");
	m_find_point.reset();
	m_find_point.bind(1, p_value, SQLITE_STATIC);
	if (m_find_point.step()) {
		//yes -- return the id
		return m_find_point.get_int64();
	}
	else {
		//no -- we need to insert it
		//iterate over the vantage points
		//  we each vp's id, the distance to the new point, and the partition bits
		sqlite_int64 partition = 0;
		std::vector<sqlite_int64> vp_ids;
		std::vector<int> dists;
		vp_ids.reserve(32); //there are at most 32 vantage points
		dists.reserve(32);

		if (!m_select_vps) {
			m_select_vps = m_db("SELECT id, bound_1, bound_2, bound_3, value FROM mvp_vantage_points ORDER by id ASC;");
		}
		if (!m_vp_count_0) {
			m_vp_count_0 = m_db("UPDATE mvp_vantage_points SET count_0 = count_0 + 1 WHERE id = $id");
			m_vp_count_1 = m_db("UPDATE mvp_vantage_points SET count_1 = count_1 + 1 WHERE id = $id");
			m_vp_count_2 = m_db("UPDATE mvp_vantage_points SET count_2 = count_2 + 1 WHERE id = $id");
			m_vp_count_3 = m_db("UPDATE mvp_vantage_points SET count_3 = count_3 + 1 WHERE id = $id");
		}
		
		m_select_vps.reset();
		m_select_vps.exec(
			[&](sqlite_int64 id, int b1, int b2, int b3, blob_view value) {
				auto dist = s_distance_fn(value, p_value);
				vp_ids.push_back(id);
				dists.push_back(dist);
				// which shell around the vantage point does the point lie in?
				// we increment that shell's count in the vantage point
				int shell;
				if (dist >= b3) {
					shell = 3;
					m_vp_count_3.reset().bind(1, id).step();
				}
				else if (dist >= b2) {
					shell = 2;
					m_vp_count_2.reset().bind(1, id).step();
				}
				else if (dist >= b1) {
					shell = 1;
					m_vp_count_1.reset().bind(1, id).step();
				}
				else {
					shell = 0;
					m_vp_count_0.reset().bind(1, id).step();
				}
				// the partition is the intersection of all of the shells
				partition |= partition_bits(shell, id);
		});
		
		// now we need to insert the point
		// INSERT INTO mvp_points(partition, value, d0, d1, ...) VALUES ($partition, $value, $d0, $d1, ...) RETURNING id;
		if (!m_insert_point) m_insert_point = make_insert_point(vp_ids);
		if (!m_add_point) m_add_point = m_db("UPDATE mvp_counts SET points = points + 1 WHERE id = 1;");

		m_insert_point.bind(1, partition);
		m_insert_point.bind(2, p_value, SQLITE_STATIC);
		for (int i = 0; i < dists.size(); ++i) {
			m_insert_point.bind(i + 3, dists[i]);
		}
		auto pt_id = m_insert_point.reset().step_get_int64();
		m_add_point.reset().step();

	}
}

sqlite_int64 MVPTable::insert_vantage_point(blob_view vp_value)
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
	
	auto insert_vp = m_db("INSERT INTO mvp_vantage_points(value) VALUES($value) RETURNING id;");
	
	int64_t vp_id;
	insert_vp.bind("$value", vp_value.data(), static_cast<int>(vp_value.size()));
	if (insert_vp.step()) {
		vp_id = insert_vp.get_int64();
		m_db.exec("UPDATE mvp_counts SET vantage_points = vantage_points + 1 WHERE id = 1;");
		insert_vp.reset();
	}
	else {
		throw std::runtime_error("Error inserting new vantage_point");
	}

	//2. add the new column of distances to mvp_points
	std::string col_name = "d" + std::to_string(vp_id);

	m_db.exec("ALTER TABLE mvp_points ADD COLUMN " + col_name + " INTEGER;");
	
	m_db.exec("CREATE INDEX mvp_idx_" + col_name + " ON mvp_points(" + col_name + ");");
	
	auto update_col = m_db("UPDATE mvp_points SET " + col_name + " = mvp_distance($vp_value, value);");
	update_col.bind(1, vp_value, SQLITE_STATIC);
	update_col.exec();

	//3. balance the shells
	balance(vp_id);

	return vp_id;
}

int64_t MVPTable::query(const blob_type& q_value, uint32_t radius)
{
	check_db();

	//Iterate over all of the vantage points
	// getting the distance from each to the query point
	// and for each shell, whether that shell intersects the query ball
	if (!m_q_sel_vps.ok()) {
		m_q_sel_vps = m_db(
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
		);
	}

	std::vector<int64_t> vp_ids;
	std::vector<int64_t> parts;
	parts.push_back(0); // which paritions the query ball covers
	
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
	sel_vps.reset();
	update_vp_ids(vp_ids);
	
	//populate the query table with the points covered by the partitions
	// sort by the distance to the query point

	//first clear the query table
	cache.exec("DELETE FROM mvp_query;");
	
	// build the query 
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
		return cache.exec_getBlob(
			"SELECT value FROM mvp_points ORDER BY partition DESC, random() LIMIT 1;", "value");
	}
	else {
		//we need to find a point that's far from most other points
		// since we don't have any vantage points yet, we will rank
		// the points by the sum of the distances to all of the other points
		// and pick the greatest
		//the static_cast to unsigned is OK because a previous case confirmed num_points > 0
		if (static_cast<size_t>(num_points) <= sample_size) {
			//we have few points, so do the pairwise distance between all
			return cache.exec_getBlob(
				"SELECT value FROM ("
				"SELECT p.value AS value, sum(mvp_distance(p.value, q.value)) AS sum_dist"
				"FROM mvp_points p, mvp_points q GROUP BY p.id"
				") ORDER BY sum_dist DESC LIMIT 1;", "value");
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
			return cache.exec_getBlob(stmt, "value");
		}
	}
}

std::vector<int64_t> MVPTable::check_balance(int64_t min_count, float threshold) 
{
	auto np = count_points();
	if (np < min_count) return {};

	//low and high thresholds for balance -- if any shell is outside these counts then it needs rebalancing
	// the /4 is because the points are divided into 4 shells around each vantage point
	auto low = np * (1.0 - threshold) / 4;
	auto high = np * (1.0 + threshold) / 4;
	//scan through the existing vantage points and check to see if their partitions are balanced
	auto& sel_vps = cache["SELECT id, count_0, count_1, count_2, count_3 FROM mvp_vantage_points ORDER BY id ASC;"];
	std::vector<int64_t> bad_ids;
	while (sel_vps.executeStep()) {
		auto id = sel_vps.getColumn("id").getInt64();
		auto count_0 = sel_vps.getColumn("count_0").getInt64();
		auto count_1 = sel_vps.getColumn("count_1").getInt64();
		auto count_2 = sel_vps.getColumn("count_2").getInt64();
		auto count_3 = sel_vps.getColumn("count_3").getInt64();
		if (count_0 < low || count_1 < low || count_2 < low || count_3 < low
			|| count_0 > high || count_1 > high || count_2 > high || count_3 > high) 
		{
			bad_ids.push_back(id);
		}
	}
	sel_vps.reset();
	return bad_ids;
}

void MVPTable::balance(int64_t vp_id)
{
	std::string col_name = "d" + std::to_string(vp_id);

	//3. choose the shell boundaries so that they're balanced
	//   we want to find the 25%, 50%, and 75% ranked distances
	
	int32_t bound_1, bound_2, bound_3;
	int64_t count_0, count_1, count_2, count_3;
	
	auto point_count = count_points();
	if (point_count >= 8) {
		auto rank_25 = point_count / 4;
		auto rank_50 = point_count / 2;
		auto rank_75 = rank_50 + rank_25;
		auto find_bound = SQLite::Statement(*db,
			"SELECT " + col_name + " FROM mvp_points ORDER BY " + col_name + " LIMIT 1 OFFSET $rank;"
		);
		find_bound.bind("$rank", rank_25);
		bound_1 = cache.exec_getInt(find_bound, 0);

		find_bound.bind("$rank", rank_50);
		bound_2 = cache.exec_getInt(find_bound, 0);

		find_bound.bind("$rank", rank_75);
		bound_3 = cache.exec_getInt(find_bound, 0);

		// the shells include the lower bound, but exclude the upper
		// so the number of points is the difference between ranks
		count_0 = rank_25;
		count_1 = rank_50 - rank_25;
		count_2 = rank_75 - rank_50;
		count_3 = point_count - rank_75;
		//if the shells collapse to zero width, then the highest shell is
		// used when partitioning and querying. 
		// However, we want to leave the counts alone here
		// so that we're not continuously trying to rebalance it
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
	upd_vp.bind("$id", vp_id);
	upd_vp.bind("$b1", bound_1);
	upd_vp.bind("$b2", bound_2);
	upd_vp.bind("$b3", bound_3);
	upd_vp.bind("$c0", count_0);
	upd_vp.bind("$c1", count_1);
	upd_vp.bind("$c2", count_2);
	upd_vp.bind("$c3", count_3);
	cache.exec(upd_vp);

	//4. Iterate over all of the points and update their partition for the new vantage point
	auto upd_points_part = SQLite::Statement(*db,
		"UPDATE mvp_points SET "
		"partition = (partition & $mask) | ("
		"CASE " // as partition_bits()
		"WHEN " + col_name + " >= $b3 THEN 3 "
		"WHEN " + col_name + " >= $b2 THEN 2 "
		"WHEN " + col_name + " >= $b1 THEN 1 "
		"ELSE 0 "
		"END << $part_off);"
	);

	upd_points_part.bind("$mask", ~partition_mask(vp_id)); //zero for this partition, ones elsewhere
	upd_points_part.bind("$part_off", partition_offset(vp_id));
	upd_points_part.bind("$b1", bound_1);
	upd_points_part.bind("$b2", bound_2);
	upd_points_part.bind("$b3", bound_3);
	cache.exec(upd_points_part);
}

void MVPTable::auto_balance(int64_t min_count, float threshold) 
{
	auto vp_ids = check_balance(min_count, threshold);
	for (auto vp_id : vp_ids) {
		balance(vp_id);
	}
}

int64_t MVPTable::auto_vantage_point(int64_t target) {
	auto np = count_points();
	//do we need to add any vantage points?
	//we want a number of vantage points proportional to the log of the number of points
	// the ratio is based roughly on the desired partition size (as each VP splits all partitions into 4)
	int64_t target_nvp = static_cast<int64_t>(std::ceil(std::log(np) / std::log(4 * target)));
	auto nvp = count_vantage_points();
	for(auto i = nvp; i < target_nvp; ++i) {
		insert_vantage_point(find_vantage_point(25));
	}
	return target_nvp;
}