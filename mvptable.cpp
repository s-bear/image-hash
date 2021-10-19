
#include "mvptable.h"
#include <sqlite3.h>
#include <algorithm>


namespace {
	
	//this bit of code happens often enough that it's worth factoring out
	inline SQLite::Column exec_get(MVPTable::stmt_ptr stmt, int col)
	{
		stmt->reset();
		if (stmt->executeStep()) {
			return stmt->getColumn(col);
		}
		else {
			throw std::runtime_error("Error executing statement");
		}
	}

	inline bool in_bounds(int32_t d, int32_t radius, int32_t low)
	{
		return d + radius >= low; //TODO: overflow
	}
	inline bool in_bounds(int32_t d, int32_t radius, int32_t low, int32_t high)
	{
		return d + radius >= low && d - radius < high; //TODO: overflow
	}
}


// Construct, open or create the database
MVPTable::MVPTable(std::shared_ptr<SQLite::Database> db, std::function<distance_fn> dist_fn) 
	: db(db), dist_fn_(dist_fn)
{
	//initialize database as necessary
	db->exec(str_init_tables);

	db->createFunction("mvp_distance", 2, true, this, MVPTable::sql_distance);
	db->createFunction("mvp_shell_r", 4, true, this, MVPTable::sql_shell_r);

	count_rows = make_stmt(str_count_rows);
	ins_counts = make_stmt(str_ins_counts);
	sel_vp_ids = make_stmt(str_sel_vp_ids);
	sel_count = make_stmt(str_sel_count);

	//if counts is empty, initialize it
	count_rows->bind("table", "mvp_counts");
	if (exec_get_int64(count_rows) == 0) {
		count_rows->bind("table", "mvp_points");
		auto num_points = exec_get(count_rows, 0).getInt64();

		count_rows->bind("table", "mvp_vantage_points");
		auto num_vantage_points = exec_get(count_rows, 0).getInt64();
		
		ins_counts->bind("points", num_points);
		ins_counts->bind("vantage_points", num_vantage_points);
		ins_counts->reset();
		ins_counts->exec();
	}

	//get all of the vantage point ids
	std::vector<int64_t> vp_ids;
	sel_vp_ids->reset();
	while (sel_vp_ids->executeStep()) {
		vp_ids.push_back(sel_vp_ids->getColumn(0).getInt64());
	}
	vp_ids_ = std::move(vp_ids);

	//precompile more statements
	increment_count = make_stmt(str_increment_count);

	ins_point = make_stmt(str_ins_point(vp_ids_));
	sel_all_points = make_stmt(str_sel_all_points);
	sel_point_by_value = make_stmt(str_sel_point_by_value);
	sel_point_by_rank = make_stmt(str_sel_point_by_rank);
	upd_point = make_stmt(str_upd_point);
	add_points_col = make_stmt(str_add_points_col);

	sel_vps = make_stmt(str_sel_vps);
	ins_vp = make_stmt(str_ins_vp);
	increment_vp_count = make_stmt(str_increment_vp_count);
	upd_vp = make_stmt(str_upd_vp);

	del_query = make_stmt(str_del_query);
	ins_query = make_stmt(str_ins_query(vp_ids));
	sel_query = make_stmt(str_sel_query);
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

void MVPTable::sql_shell_r(sqlite3_context* ctx, int n, sqlite3_value* args[])
{
	if (n != 4) {
		sqlite3_result_error(ctx, "mvp_shell requires 4 args.", -1);
		return;
	}
	auto bound_1 = sqlite3_value_int(args[0]);
	auto bound_2 = sqlite3_value_int(args[1]);
	auto bound_3 = sqlite3_value_int(args[2]);
	auto d = sqlite3_value_int(args[3]);
	int shell;
	if (in_bounds(d, 0, bound_3)) shell = 3;
	else if (in_bounds(d, 0, bound_2, bound_3)) shell = 2;
	else if (in_bounds(d, 0, bound_1, bound_2)) shell = 1;
	else if (in_bounds(d, 0, 0, bound_1)) shell = 0;
	else {
		sqlite3_result_error(ctx, "mvp_shell distance error", -1);
		return;
	}
	sqlite3_result_int(ctx, 3 - shell);
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
		"INSERT INTO temp.mvp_query(id, dist)"
		"SELECT id, mvp_distance($q_value, value)"
		"FROM mvp_points WHERE partition = $partition";
	for (int64_t id : vp_ids) {
		auto id_str = std::to_string(id);
		stmt += " AND (d" + id_str + " BETWEEN ? AND ?)";
	}
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

MVPTable::stmt_ptr MVPTable::make_stmt(const std::string stmt)
{
	if (db == nullptr) throw std::runtime_error("No database connection");
	return std::make_unique<SQLite::Statement>(*db, stmt);
}

void MVPTable::update_vp_ids(const std::vector<int64_t>& vp_ids)
{
	if (!std::equal(vp_ids_.begin(), vp_ids_.end(), vp_ids.begin(), vp_ids.end())) {
		vp_ids_ = vp_ids;
		ins_point.reset();
	}
}

void MVPTable::exec_increment_count(const std::string& table) {
	increment_count->bind("col", table);
	increment_count->reset();
	increment_count->exec();
}


int64_t MVPTable::insert_point(const blob_type& p_value)
{
	//is the point already in the database?
	sel_point_by_value->bind("value", p_value.data(), p_value.size());
	sel_point_by_value->reset();
	if (sel_point_by_value->executeStep()) {
		//yes
		return sel_point_by_value->getColumn(0).getInt64();
	}
	else {
		//no: we need to add the point

		//iterate over the vantage points, calculating the distance from each to p_value
		// and calculating which parition p_value falls into
		std::vector<int64_t> vp_ids;
		std::vector<int32_t> dists;
		int64_t part = 0;
		sel_vps->reset();
		while (sel_vps->executeStep()) {
			auto id = sel_vps->getColumn("id").getInt64();
			int32_t bound_1 = sel_vps->getColumn("bound_1").getInt();
			int32_t bound_2 = sel_vps->getColumn("bound_2").getInt();
			int32_t bound_3 = sel_vps->getColumn("bound_3").getInt();
			auto vp_value = get_blob(sel_vps->getColumn("value"));

			vp_ids.push_back(id);
			uint32_t d = distance(vp_value, p_value);
			dists.push_back(d);
			//which shell around the vantage point does this point fall into
			// also prepare the increment_vp_count statement for incrementing the occupancy count of that shell
			increment_vp_count->bind("id", id);
			int64_t shell = -1;
			if (in_bounds(d, 0, bound_3)) {
				shell = 3;
				increment_vp_count->bind("col", "count_3");
			}
			else if (in_bounds(d, 0, bound_2, bound_3)) {
				shell = 2;
				increment_vp_count->bind("col", "count_2");
			}
			else if (in_bounds(d, 0, bound_1, bound_2)) {
				shell = 1;
				increment_vp_count->bind("col", "count_1");
			}
			else if (in_bounds(d, 0, 0, bound_1)) {
				shell = 0;
				increment_vp_count->bind("col", "count_0");
			}
			else {
				throw std::runtime_error("Error inserting point: invalid distance");
			}

			//increment the count for that shell
			increment_vp_count->reset();
			increment_vp_count->exec();

			//calculate the partition
			//each vp has 4 shells, so we index by multiples of 4 based on the vp's id
			// that is, each vp gets [4*id, 4*id + 3] to store its shell
			// we index by shell in reverse order because by default (before partitioning) all of the
			// points fall in shell 3, so this way adding a new vantage point doesn't require repartitioning
			part += (3-shell)*4*id;
		}

		//update the insert_point statement if vp_ids changed
		update_vp_ids(vp_ids);
		if (!ins_point) ins_point = make_stmt(str_ins_point(vp_ids));

		ins_point->bind("part", part);
		ins_point->bind("value", p_value.data(), p_value.size());
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
	// by default, the bounds are all 0, so all of the points will fall into shell 3
	// so count_3 needs to be set to the total number of points
	// the way parition indexing works, adding the new vantage point does not require
	// updating any existing partitions
	sel_count->bind("col", "points");
	int64_t point_count = exec_get(sel_count, 0).getInt64();

	int64_t vp_id;
	ins_vp->bind("count_3", point_count);
	ins_vp->bind("value", vp_value.data(), vp_value.size());
	ins_vp->reset();
	if (ins_vp->executeStep()) {
		exec_increment_count("vantage_points");
		vp_id = ins_vp->getColumn("id").getInt64();
	}
	else {
		throw std::runtime_error("Error inserting new vantage_point");
	}

	//2. add the new column of distances to mvp_points
	//   this uses the mvp_distance callback to calculate the 
	//   distance from vp_value to each point
	std::string col_name = "d" + std::to_string(vp_id);

	add_points_col->bind("col", col_name);
	add_points_col->bind("idx", "mvp_idx_" + col_name);
	add_points_col->bind("vp_value", vp_value.data(), vp_value.size());
	add_points_col->reset();
	add_points_col->exec();

	//3. balance the partitions
	// we want to find the 25%, 50%, and 75% distances
	// which will be the partition boundaries
	int64_t rank_25 = point_count / 4;
	int64_t rank_50 = point_count / 2;
	int64_t rank_75 = rank_50 + rank_25;
	sel_point_by_rank->bind("col", col_name);
	sel_point_by_rank->bind("rank", rank_25);
	auto bound_1 = exec_get(sel_point_by_rank,0).getInt();
	sel_point_by_rank->bind("rank", rank_50);
	auto bound_2 = exec_get(sel_point_by_rank,0).getInt();
	sel_point_by_rank->bind("rank", rank_75);
	auto bound_3 = exec_get(sel_point_by_rank,0).getInt();
	//the boundaries are inclusive lower bounds
	// so the number of points in each are less one
	int64_t count_0 = rank_25 - 1;
	int64_t count_1 = rank_50 - rank_25;
	int64_t count_2 = rank_75 - rank_50;
	int64_t count_3 = point_count - rank_75;
	upd_vp->bind("bound_1", bound_1);
	upd_vp->bind("bound_2", bound_2);
	upd_vp->bind("bound_3", bound_3);
	upd_vp->bind("count_0", count_0);
	upd_vp->bind("count_1", count_1);
	upd_vp->bind("count_2", count_2);
	upd_vp->bind("count_3", count_3);
	upd_vp->reset();
	upd_vp->exec();

	add_points_partition->bind("b1", bound_1);
	add_points_partition->bind("b2", bound_2);
	add_points_partition->bind("b3", bound_3);
	add_points_partition->bind("col", col_name);
	add_points_partition->bind("vp_id", vp_id);
	add_points_partition->reset();
	add_points_partition->exec();
}

std::vector<int64_t> MVPTable::query(const blob_type& q_value, uint32_t radius, int64_t limit)
{
	// Get the distance from the query point to each vantage point
	// and get the parititions that the query covers
	std::vector<int64_t> vp_ids;
	std::vector<int32_t> dists;
	std::vector<int64_t> parts;
	parts.push_back(0);
	sel_vps->reset();
	while (sel_vps->executeStep()) {
		auto id = sel_vps->getColumn("id").getInt64();
		int32_t bound_1 = sel_vps->getColumn("bound_1").getInt();
		int32_t bound_2 = sel_vps->getColumn("bound_2").getInt();
		int32_t bound_3 = sel_vps->getColumn("bound_3").getInt();
		auto vp_value = get_blob(sel_vps->getColumn("value"));

		vp_ids.push_back(id);
		int32_t d = dist_fn_(vp_value, q_value);
		dists.push_back(d);

		std::vector<int> shells;
		if (in_bounds(d, 0, bound_3)) {
			shells.push_back(3);
		}
		else if (in_bounds(d, 0, bound_2, bound_3)) {
			shells.push_back(2);
		}
		else if (in_bounds(d, 0, bound_1, bound_2)) {
			shells.push_back(1);
		}
		else if (in_bounds(d, 0, 0, bound_1)) {
			shells.push_back(0);
		}
		else {
			throw std::runtime_error("Error inserting point: invalid distance");
		}

		if (shells.size() == 1) {
			//a single shell -- we can modify the existing partitions in place
			for (auto& p : parts) {
				p += (3 - shells[0]) * 4 * id;
			}
		}
		else {
			//multple shells -- the number of partitions the query covers will grow
			std::vector<int64_t> new_parts;
			new_parts.reserve(parts.size() * shells.size());
			for (auto p : parts) {
				for (auto s : shells) {
					new_parts.push_back(p + (3 - s) * 4 * id);
				}
			}
			parts = std::move(new_parts);
		}
	}
	
	update_vp_ids(vp_ids);
	if (!ins_query) ins_query = make_stmt(str_ins_query(vp_ids));

	//TODO: cull points based on their distance to the vantage points too
	//  make str_ins_query dynamic and add
	//    (d0 BETWEEN ? AND ?) ... to the WHERE term

	//populate the query table with the points covered by the partitions
	// sort by the distance to the query point

	//first clear the query table
	del_query->reset();
	del_query->exec();
	
	ins_query->bind("q_value", q_value.data(), q_value.size());
	for (size_t i = 0; i < dists.size(); ++i) {
		// bind (d{vp_ids[i]} BETWEEN ? AND ?)
		ins_query->bind(2 * i + 3, dists[i] - radius);
		ins_query->bind(2 * i + 4, dists[i] + radius); //TODO: overflow
	}
	//run the query for each partition that the radius covers
	for (auto p : parts) {
		ins_query->bind("partitions", p);
		ins_query->reset();
		ins_query->exec();
	}
	//get the points closest to the query point, within the radius
	std::vector<int64_t> result;
	sel_query->bind("radius", radius);
	sel_query->bind("limit", limit);
	sel_query->reset();
	while (sel_query->executeStep()) {
		result.push_back(sel_query->getColumn(0).getInt64());
	}
	return result;
}

MVPTable::blob_type MVPTable::find_vantage_point(size_t sample_size)
{
	// do we have any vantage points yet?
	sel_count->bind("col", "vantage_points");
	int64_t num_vantage_points = exec_get_int64(sel_count);

	if (num_vantage_points > 0) {
		//we need to find a point that's far from all of the existing vantage points

	}
	else {
		//we need to find a point that's far from all other points
	}
}
