
#include "db.h"
#include "SQLiteCpp/SQLiteCpp.h"

namespace imghash {
	class Database::Impl {
	public:
		SQLite::Database db;
		std::unique_ptr<SQLite::Statement> insert_files, insert_points, insert_vp, get_ps, get_vps, add_points_col, update_point;

		Impl(const std::string& path)
			: db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE),
			insert_files(nullptr)
		{
			//initialize database as necessary
			SQLite::Transaction trans(db);
			db.exec("CREATE TABLE IF NOT EXISTS files ("
				"path TEXT PRIMARY KEY,"
				"hash BLOB"
				") WITHOUT ROWID;"
			);
			db.exec("CREATE INDEX IF NOT EXISTS "
				"idx_files_hash ON files(hash);"
			);
			db.exec("CREATE TABLE IF NOT EXISTS points ("
				"id INTEGER PRIMARY KEY,"
				"hash BLOB"
				");"
			);
			db.exec("CREATE TABLE IF NOT EXISTS vantage_points ("
				"id INTEGER PRIMARY KEY,"
				"hash BLOB"
				");"
			);
			trans.commit();
		}

		std::unique_ptr<SQLite::Statement> make_stmt(const std::string& stmt)
		{
			return std::make_unique<SQLite::Statement>(db, stmt);
		}

		std::unique_ptr<SQLite::Statement> make_insert_points(const std::vector<int64_t>& ids)
		{
			std::string stmt1 = "INSERT INTO points(hash";
			std::string stmt2 = ") VALUE(?";
			for (int64_t id : ids) {
				auto id_str = std::to_string(id);
				stmt1 += ", d" + id_str;
				stmt2 += ", ?";
			}
			stmt2 += ");";
			return make_stmt(stmt1 + stmt2);
		}

		Hash::hash_type get_hash(SQLite::Column& col) {
			size_t n = col.getBytes();
			const uint8_t* data = static_cast<const uint8_t*>(col.getBlob());
			return Hash::hash_type(data, data + n);
		}

		void insert(const Hash::hash_type& p_hash, const std::string& path)
		{
			//Calculate distances to each vantage point
			if (!get_vps)
				get_vps = make_stmt("SELECT * FROM vantage_points ORDER BY id ASC;");
			
			get_vps->reset();
			std::vector<int64_t> ids;
			std::vector<uint32_t> dists;
			while (get_vps->executeStep()) {
				auto id = get_vps->getColumn(0).getInt64(); //vantage point id
				ids.push_back(id);
				//copy the blob into a hash_type
				//TODO: use span to avoid copies
				auto vp_hash = get_hash(get_vps->getColumn(1));

				uint32_t d = Hash::hamming_distance(vp_hash, p_hash);
				dists.push_back(d);
			}
			//Prepare insertion into files
			if (!insert_files)
				insert_files = make_stmt("INSERT OR REPLACE INTO files VALUES(:path, :hash);");

			insert_files->reset();
			insert_files->bind("path", path);
			insert_files->bind("hash", p_hash.data(), p_hash.size());
			//Prepare insertion into points
			if (!insert_points)
				insert_points = make_insert_points(ids);
			insert_points->reset();
			insert_points->bind(1, p_hash.data(), p_hash.size());
			for (size_t i = 0; i < dists.size(); ++i) {
				insert_points->bind(i + 2, dists[i]); //the first parameter has index 1, so these start at 2
			}

			//Perform the insertions
			SQLite::Transaction trans(db);
			insert_files->exec();
			insert_points->exec();
			trans.commit();
		}

		void add_vp(const Hash::hash_type& vp_hash)
		{
			//Prepare insertion into vantage_points
			if (!insert_vp)
				insert_vp = make_stmt("INSERT INTO vantage_points(hash) VALUES(:hash) RETURNING id;");

			if (!add_points_col)
				add_points_col = make_stmt("ALTER TABLE points ADD COLUMN :col INTEGER DEFAULT 0xFFFFFFFF;"
					"CREATE INDEX :idx ON points(:col);");

			if (!get_ps)
				get_ps = make_stmt("SELECT id,hash FROM points;");

			if (!update_point)
				update_point = make_stmt("UPDATE points SET :col = :val WHERE id = :id");

			insert_vp->reset();
			add_points_col->reset();
			get_ps->reset();
			update_point->reset();

			insert_vp->bind("hash", vp_hash.data(), vp_hash.size());
			//we need to get the new vantage point's ID before we can bind add_points_col's parameters
			// so it will need to happen within the transaction
			SQLite::Transaction trans(db);
			if (!insert_vp->executeStep()) {
				throw std::runtime_error("Error inserting vantage point");
			}
			uint64_t id = insert_vp->getColumn(0).getInt64();
			std::string col_name = "d" + std::to_string(id);
			add_points_col->bind("col", col_name);
			add_points_col->bind("idx", "idx_" + col_name);
			
			add_points_col->exec();
			
			//the database is almost in a usable state again, but we need to compute all of the distances
			// but it won't return correct results for the newest vantage point yet
			update_point->bind("col", col_name);
			while (get_ps->executeStep()) {
				auto id = get_ps->getColumn(0).getInt64();
				auto p_hash = get_hash(get_ps->getColumn(1));
				
				uint32_t d = Hash::hamming_distance(vp_hash, p_hash);
				update_point->bind("id", id);
				update_point->bind("val", d);
				update_point->exec();
			}

			trans.commit();
		}
	};

	//Open the database
	Database::Database(const std::string& path)
		: impl(std::make_unique<Impl>(path))
	{
		//nothing else to do
	}

	//Close the database
	Database::~Database()
	{

	}

	//Add a file
	void Database::insert(const Hash::hash_type& hash, const std::string& name)
	{
		impl->insert(hash, name);
	}

	//Find similar images
	void Database::query(const Hash::hash_type& hash, unsigned int dist)
	{

	}

	//Add a vantage point for querying
	void Database::add_vantage_point(const Hash::hash_type& hash)
	{
		impl->add_vp(hash);
	}

	//Find a point that would make a good vantage point
	Hash::hash_type Database::find_vantage_point()
	{

	}
	
}