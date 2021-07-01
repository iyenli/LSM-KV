#include "kvstore.h"

/*
* Until 5.30, TODO:
* 1. 
*
*
**/
KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
	try
	{
		// if dir not exist, throw error.
		if (!utils::dirExists(dir))
		{
			throw "error directory";
			exit(0);
		}

		cacheptr.clear();

		// first level
		cacheptr.resize(1);
		directory = dir;
		memtable = new skiptable(dir);

		string initpath = dir + "/level_0";

		// the second_level_filename-directory under dir.
		first_dir.clear();
		first_level_amount = utils::scanDir(dir, first_dir);

		sort(first_dir.begin(), first_dir.end(), level1_cmp);

		// second_level_filename 0 don't exist.
		if (!first_level_amount)
		{
			utils::_mkdir(initpath.data());
			first_dir.clear();
			first_level_amount = utils::scanDir(dir, first_dir);
		}

		// sst number in each level
		vector<int> second_levelnum;
		// file name in each hieararchy
		vector<vector<string>> second_level_filename;
		// read timestamp
		// file name in every sub directory
		second_level_filename.resize(first_level_amount);
		second_levelnum.resize(first_level_amount);

		for (int i = 0; i < first_level_amount; ++i)
		{
			string sstPath = dir + "/" + first_dir[i];
			second_level_filename[i].clear();
			second_levelnum[i] = utils::scanDir(sstPath, second_level_filename[i]);
			sort(second_level_filename[i].begin(),
				 second_level_filename[i].end(), level2_cmp);
		}

		// open files and readin
		fstream fs;

		// find max timestamp
		uint64_t maxstamp = 0;

		cachenode *tmpptr;
		for (int i = 0; i < first_level_amount; ++i)
		{
			tmpptr = new cachenode;
			for (int j = 0; j < second_levelnum[i]; ++j)
			{
				initpath = dir + "/" + first_dir[i] + "/" + second_level_filename[i][j];
				fs.open(initpath, fstream::binary | fstream::in);

				// read meta data
				fs.read((char *)(&(tmpptr->timestamp)), 8);
				fs.read((char *)(&(tmpptr->amount)), 8);
				fs.read((char *)(&(tmpptr->min_val)), 8);
				fs.read((char *)(&((tmpptr->max_val))), 8);

				// read bloom set
				fs.read(tmpptr->bloomset->bits, 10240);

				// read key-offset
				tmpptr->key_offset = new keyO[tmpptr->amount];
				fs.read((char *)(tmpptr->key_offset), 12 * tmpptr->amount);

				// is the max time stamp?
				maxstamp = max(maxstamp, tmpptr->timestamp);

				// add pointers of cache to the vector
				cacheptr[i].push_back(tmpptr);

				// close the file
				fs.close();
			}
		}

		// set new time stamp to skiptable.
		memtable->init(dir, maxstamp);
	}
	catch (std::bad_alloc)
	{
		printf("KV CONSTRUCT ERROR");
	}
	return;
}

KVStore::~KVStore()
{
	// TODO: when program terminate, store all in MEM to DISK
	// memtable->toDisk();
	memtable->clear();
	delete memtable;
	clear();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	// TODO: if disk is changed, update cache
	uint64_t addedStamp = 0, minval = 0;
	if (memtable->put(key, s))
	{
		// only level 0 may be changed!
		// if level 0's over limit, use compaction.
		// we update every level's cache in function
		// compaction. Now, ONLY consider one file added.
		first_dir.clear();
		first_level_amount = utils::scanDir(directory, first_dir);
		sort(first_dir.begin(), first_dir.end(), level1_cmp);
		string path = directory + "/" + first_dir[0];
		// scanDir will APPEND filename to the vector instead of replacing!
		// so we HAVE TO update it manually.

		// path += "/" + second_level_filename[0].back();
		cachenode *tmpptr = new cachenode;
		tmpptr->min_val = minval;
		tmpptr->timestamp = addedStamp;

		string filename = tmpptr->generate_name();
		path += filename;

		fstream fs;
		fs.open(path, fstream::binary | fstream::in);
		// read meta data
		// TODO: optimize!!
		fs.read((char *)(&(tmpptr->timestamp)), 8);
		fs.read((char *)(&(tmpptr->amount)), 8);
		fs.read((char *)(&(tmpptr->min_val)), 8);
		fs.read((char *)(&((tmpptr->max_val))), 8);

		// read bloom set
		fs.read(tmpptr->bloomset->bits, 10240);

		// read key-offset
		tmpptr->key_offset = new keyO[tmpptr->amount];
		fs.read((char *)(tmpptr->key_offset), 12 * tmpptr->amount);

		// add pointers of cache to the vector
		cacheptr[0].push_back(tmpptr);
		// close the file
		fs.close();
	}

	// TODO: add compaction after finishing it.
	// compaction(0);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	string ret = memtable->get(key);

	// find in sstable, return.
	if (ret == "")
	{
		first_dir.clear();
		first_level_amount = utils::scanDir(directory, first_dir);
		sort(first_dir.begin(), first_dir.end(), level1_cmp);
		if (cacheptr.size() == 0)
			return ret;
		// SORT ZERO USING
		sort(cacheptr[0].begin(), cacheptr[0].end(), cache_cmp_timestamp);
		for (long i = 0; i < cacheptr[0].size() && ret == ""; ++i)
		{
			if (key > cacheptr[0][i]->max_val || key < cacheptr[0][i]->min_val)
				continue;
			if (!cacheptr[0][i]->bloomset->Contain(key))
			{
				continue;
			}

			uint64_t left = 0, right = cacheptr[0][i]->amount - 1, middle, currkey;
			uint32_t offset, next_offset;

			while (left <= right)
			{
				middle = (left + right) / 2;
				currkey = cacheptr[0][i]->key_offset[middle].getkey();

				// hit
				if (currkey == key)
				{
					offset = cacheptr[0][i]->key_offset[middle].getoffset();
					next_offset = (middle + 1 == cacheptr[0][i]->amount)
									  ? 0
									  : cacheptr[0][i]->key_offset[middle + 1].getoffset();

					// get file name
					string sstname = directory + "/" + first_dir[0] + cacheptr[0][i]->generate_name();

					sstable stable(sstname);
					ret = stable.search(offset, next_offset);

					break;
				}
				if (currkey > key)
				{
					if (middle == 0)
						break;
					right = middle - 1;
				}
				else
					left = middle + 1;
			}
		}

		for (int i = 1; i < first_level_amount && ret == ""; ++i)
		{
			sort(cacheptr[i].begin(), cacheptr[i].end(), cache_cmp_minval);
			uint64_t highleft = 0, highright = cacheptr[i].size() - 1, highmiddle;

			while (highright >= highleft)
			{
				highmiddle = (highleft + highright) / 2;
				if (key <= cacheptr[i][highmiddle]->max_val &&
					key >= cacheptr[i][highmiddle]->min_val)
				{
					// if file [i][middle] doesn't contain key, skip to next layer directly!
					if (!cacheptr[i][highmiddle]->bloomset->Contain(key))
						break;

					uint64_t left = 0, right = cacheptr[i][highmiddle]->amount - 1, middle, currkey;
					uint32_t offset, next_offset;

					while (left <= right)
					{
						middle = (left + right) / 2;
						currkey = cacheptr[i][highmiddle]->key_offset[middle].getkey();

						// hit
						if (currkey == key)
						{
							offset = cacheptr[i][highmiddle]->key_offset[middle].getoffset();

							next_offset = (middle + 1 == cacheptr[i][highmiddle]->amount)
											  ? 0
											  : cacheptr[i][highmiddle]->key_offset[middle + 1].getoffset();

							// get file name
							string sstname = directory + "/" + first_dir[i] + cacheptr[i][highmiddle]->generate_name();

							sstable stable(sstname);
							ret = stable.search(offset, next_offset);

							break;
						}
						if (currkey > key)
						{
							if (middle == 0)
								break;
							right = middle - 1;
						}
						else
							left = middle + 1;
					}
				}
			}
		}
		if (ret == DELSIG)
			ret = "";
		return ret;
	}
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false if the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	bool flag = (get(key) == "");
	if (flag)
		return !flag;
	memtable->del(key);
	put(key, DELSIG);
	return !flag;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	memtable->reset();
	// clear all .sst tables
	string deletename = directory;
	for (int i = 0; i < first_level_amount; ++i)
	{
		for (long j = 0; j < cacheptr[0].size(); ++j)
		{
			deletename = directory + "/" +
						 first_dir[i] + cacheptr[i][j]->generate_name();
			utils::rmfile(deletename.data());
		}
		deletename = directory + "/" +
					 first_dir[i];
		utils::rmdir(deletename.data());
	}
	clear();
}

/**
 * compaction function
 * 1. conduct with THIS level and next level
 * 2. first_dirname and second_file_name all sequential!(level 0 - level 7 - level 10)
 * 		or (1.sst - 5.sst - 10.sst)
 * 3. cacheptr is as the same sequential way
 */
// void KVStore::compaction(int level)
// {
// 	// get directory name
// 	string sstname = directory + "/level_";

// 	stringstream tmpstream;
// 	tmpstream << level;
// 	sstname += tmpstream.str();
// 	tmpstream.str("");
// 	tmpstream.clear();

// 	// if directory not exist.
// 	if (!utils::dirExists(sstname))
// 		return;

// 	// recur end: no need to expand.
// 	long max_file_number = pow(2, level + 1);
// 	if (cacheptr[level].size() <= max_file_number)
// 		return;

// 	// need to pick some to next level, so:
// 	string sublevel = directory + "/level_";

// 	tmpstream << level + 1;
// 	sublevel += tmpstream.str();

// 	// who will attend?
// 	long topbeg = -1, topend = -1;		 // contain begin and end!!
// 	long bottombeg = -1, bottomend = -1; // contain begin and end!!

// 	// example: sstname="./data/level_6" and sublevel = "./data/level_7"
// 	if (!utils::dirExists(sublevel))
// 	{
// 		utils::_mkdir(sublevel.data());

// 		++first_level_amount;

// 		// what's more: create new cacheptr level;
// 		vector<cachenode *> tmp(0);
// 		cacheptr.push_back(tmp);

// 		// no files in next level!
// 		bottombeg = -2, bottomend = -2;
// 	}

// 	uint64_t minkey = UINT64_MAX, maxkey = 0;

// 	// topl level
// 	if (level == 0)
// 	{
// 		// cacheptr[0][0~2]; only consider 3 FILES in l0
// 		topbeg = 0, topend = 2;
// 	}
// 	else
// 	// 选择时间戳最小的文件，若时间戳相同，则选择键最小的文件
// 	{
// 		// ptr switch: need compaction, sorted by timestamp
// 		// need get value, use min_val to sort
// 		sort(cacheptr[level].begin(), cacheptr[level].end(), cache_cmp_timestamp);
// 		topbeg = 0, topend = cacheptr[level].size() - max_file_number - 1;
// 		for (int i = topbeg; i <= topend; ++i)
// 		{
// 			if (cacheptr[level][i]->min_val < minkey)
// 				minkey = cacheptr[level][i]->min_val;
// 			if (cacheptr[level][i]->max_val > maxkey)
// 				maxkey = cacheptr[level][i]->max_val > maxkey;
// 		}
// 	}

// 	// originally exist the bottom level
// 	if (bottombeg == -1 && cacheptr[level + 1].size() != 0)
// 	{
// 		// next level, we need min_val method to sort
// 		sort(cacheptr[level + 1].begin(), cacheptr[level + 1].end(),
// 			 cache_cmp_minval);

// 		// 寻找与其有交集的
// 		long left = 0, right = cacheptr[level + 1].size() - 1;

// 		if (cacheptr[level + 1][right]->max_val < minkey ||
// 			cacheptr[level + 1][left]->min_val > maxkey)
// 			;
// 		else
// 			while (1)
// 			{
// 				bool flag = true;
// 				if (cacheptr[level + 1][left]->max_val < minkey)
// 				{
// 					++left;
// 					flag = false;
// 				}
// 				if (cacheptr[level + 1][right]->min_val > maxkey)
// 				{
// 					--right;
// 					flag = false;
// 				}
// 				if (flag)
// 				{
// 					bottombeg = left;
// 					bottomend = right;
// 					break;
// 				}
// 			}

// 		/**
// 		*  after this processor, bottom begin / end has several meanings:
// 		*  1. both -2: NO NEXT LEVEL FILES EXIST
// 		*  2. over 0 / size()-1: no file need attend sort but files exist
// 		*  3. both 0 - size() - 1 : some files need to sort
// 		*  4. attention: beg and end are all contained!!
// 		**/
// 	}

// 	// 归并排序
// 	// priority queue: find the smallest key, if key is duplicated,
// 	// only append the smallest timestamp
// 	// create a new memtable and insert once again.
// 	// write into disk when overloading

// 	// maintain vector pointer[] to point at (topend - topbeg + 1 ) +
// 	//  (-1? -2? 0) : bottomend - bottombeg + 1 ;
// 	vector<cachenode *> pointer;
// 	long fileNum = topend - topbeg + 1;
// 	fileNum += (bottombeg == -1 || bottombeg == -2)
// 				   ? 0
// 				   : bottomend - bottombeg + 1;
// 	pointer.clear();
// 	for (long i = topbeg; i <= topend; ++i)
// 	{
// 		pointer.push_back(cacheptr[level][i]);
// 		stringstream ss;
// 		ss.str("");
// 		ss.clear();
// 		ss << level;
// 		pointer[i]->level = level;
// 	}

// 	if (bottombeg != -1 && bottombeg != -2)
// 	{
// 		for (long i = bottombeg; i <= bottomend; ++i)
// 		{
// 			pointer.push_back(cacheptr[level + 1][i]);
// 			stringstream ss;
// 			ss.str("");
// 			ss.clear();
// 			ss << level + 1;
// 			pointer[i]->level = level + 1;
// 		}
// 	}

// 	uint64_t timestamp = 0;
// 	for (long i = 0; i < fileNum; ++i)
// 	{
// 		// from 0th key begins
// 		pointer[i]->currpointer = 0;

// 		string fname = directory + pointer[i]->generate_level() +
// 					   pointer[i]->generate_name();

// 		// just remember to destruct, important!!
// 		pointer[i]->stable = new sstable(fname);

// 		timestamp = max(timestamp, pointer[i]->timestamp);
// 	}

// 	// 写入新文件
// 	long curr;
// 	uint64_t amount = 0;
// 	uint64_t currsize = initSize;

// 	vector<uint64_t> keyvector;
// 	vector<string> valuevector;

// 	uint32_t offset, next_offset;

// 	while (pointer.size() != 0)
// 	{
// 		// reassure
// 		sort(pointer.begin(), pointer.end(), cache_cmp_compaction);
// 		curr = 0;
// 		while (1)
// 		{
// 			if (curr + 1 == pointer.size() ||
// 				pointer[curr]->key_offset[pointer[curr]->currpointer].getkey() !=
// 					pointer[curr + 1]->key_offset[pointer[curr]->currpointer].getkey())
// 			{
// 				offset = pointer[curr]->key_offset[pointer[curr]->currpointer].getoffset();
// 				next_offset = (pointer[curr]->currpointer + 1 == pointer[curr]->amount)
// 								  ? 0
// 								  : pointer[curr]->key_offset[pointer[curr]->currpointer + 1].getoffset();

// 				// next offset = 0 is handled in stable
// 				string string_value = pointer[curr]->stable->search(offset, next_offset);

// 				// write into disk
// 				if (currsize + naviSize + string_value.size() > maxSize)
// 				{
// 					cachenode *newcache = new cachenode;

// 					newcache->key_offset = new keyO[amount];
// 					uint64_t min_val = keyvector[0], max_val = keyvector[amount - 1];

// 					bloom bfilter;

// 					// file path and name
// 					stringstream transfer;
// 					string path = directory + "/level_";
// 					transfer << level + 1;
// 					path += transfer.str() + "/";
// 					transfer.str("");
// 					transfer.clear();
// 					transfer << timestamp;
// 					path = path + transfer.str() + "_";
// 					transfer.str("");
// 					transfer.clear();
// 					transfer << min_val;
// 					path += transfer.str() + ".sst";

// 					// open file in binary mode
// 					fstream fs;
// 					fs.open(path, fstream::binary | fstream::out | fstream::trunc);

// 					vector<uint32_t> offsetvector;
// 					uint32_t offset = initSize + naviSize * amount;
// 					offsetvector.push_back(offset);
// 					bfilter.add(keyvector[0]);

// 					for (uint64_t i = 1; i < amount; ++i)
// 					{
// 						bfilter.add(keyvector[i]);
// 						offset += valuevector[i - 1].size();
// 						offsetvector.push_back(offset);
// 					}

// 					// init the cache key_offset
// 					for (uint64_t i = 0; i < amount; ++i)
// 					{
// 						keyO tmp_keyoffset(keyvector[i], offsetvector[i]);
// 						newcache->key_offset[i] = tmp_keyoffset;
// 					}

// 					// init the cache node
// 					newcache->bloomset = new bloom;
// 					*(newcache->bloomset) = bfilter;

// 					// write it into the file!
// 					// timestamp
// 					fs.write((const char *)(&timestamp), 8);

// 					// amount of KV pair
// 					fs.write((const char *)(&amount), 8);

// 					// max & min
// 					fs.write((const char *)(&min_val), 8);
// 					fs.write((const char *)(&max_val), 8);

// 					//bloom filter
// 					fs.write((const char *)bfilter.bits, 10240);

// 					for (uint64_t i = 0; i < offsetvector.size(); ++i)
// 					{
// 						fs.write((const char *)&keyvector[i], 8);
// 						fs.write((const char *)&offsetvector[i], 4);
// 					}

// 					for (uint64_t i = 0; i < offsetvector.size(); ++i)
// 					{
// 						fs.write(valuevector[i].c_str(), valuevector[i].size());
// 					}

// 					// put cache node into the cacheptr!
// 					newcache->max_val = max_val;
// 					newcache->min_val = min_val;
// 					newcache->amount = amount;
// 					newcache->timestamp = timestamp;

// 					// push in the cacheptr
// 					cacheptr[level + 1].push_back(newcache);

// 					// init the situation:
// 					amount = 0;
// 					currsize = initSize;
// 					keyvector.clear();
// 					valuevector.clear();
// 					offsetvector.clear();
// 				}

// 				++amount;
// 				currsize += (naviSize + string_value.size());

// 				keyvector.push_back(pointer[curr]->key_offset[pointer[curr]->currpointer + 1].getkey());
// 				valuevector.push_back(string_value);

// 				// delete from pointer if only no key left
// 				if (pointer[curr]->currpointer + 1 == pointer[curr]->amount)
// 				{
// 					// clear old cache!!
// 					// first clear from cacheptr
// 					// then clear the node!
// 					for (uint64_t i = 0; i < cacheptr[pointer[curr]->level].size(); ++i)
// 						if (pointer[curr] == cacheptr[pointer[curr]->level][i])
// 						{
// 							delete pointer[curr]->stable;
// 							string deletePath = directory +
// 												cacheptr[pointer[curr]->level][i]->generate_level() +
// 												cacheptr[pointer[curr]->level][i]->generate_name();
// 							utils::rmfile(deletePath.data());
// 							cacheptr[pointer[curr]->level].erase(cacheptr[pointer[curr]->level].begin() + i);
// 							break;
// 						}
// 					delete pointer[curr]->bloomset;
// 					delete[] pointer[curr]->key_offset;
// 					delete pointer[curr];
// 					pointer.erase(pointer.begin() + curr);
// 					if (pointer.size() == 0)
// 					{
// 						cachenode *newcache = new cachenode;

// 						newcache->key_offset = new keyO[amount];
// 						uint64_t min_val = keyvector[0], max_val = keyvector[amount - 1];

// 						bloom bfilter;

// 						// file path and name
// 						stringstream transfer;
// 						string path = directory + "/level_";
// 						transfer << level + 1;
// 						path += transfer.str() + "/";
// 						transfer.str("");
// 						transfer.clear();
// 						transfer << timestamp;
// 						path = path + transfer.str() + "_";
// 						transfer.str("");
// 						transfer.clear();
// 						transfer << min_val;
// 						path += transfer.str() + ".sst";

// 						// open file in binary mode
// 						fstream fs;
// 						fs.open(path, fstream::binary | fstream::out | fstream::trunc);

// 						vector<uint32_t> offsetvector;
// 						uint32_t offset = initSize + naviSize * amount;
// 						offsetvector.push_back(offset);
// 						bfilter.add(keyvector[0]);

// 						for (uint64_t i = 1; i < amount; ++i)
// 						{
// 							bfilter.add(keyvector[i]);
// 							offset += valuevector[i - 1].size();
// 							offsetvector.push_back(offset);
// 						}

// 						// init the cache key_offset
// 						for (uint64_t i = 0; i < amount; ++i)
// 						{
// 							keyO tmp_keyoffset(keyvector[i], offsetvector[i]);
// 							newcache->key_offset[i] = tmp_keyoffset;
// 						}

// 						// init the cache node
// 						newcache->bloomset = new bloom;
// 						*(newcache->bloomset) = bfilter;

// 						// write it into the file!
// 						// timestamp
// 						fs.write((const char *)(&timestamp), 8);

// 						// amount of KV pair
// 						fs.write((const char *)(&amount), 8);

// 						// max & min
// 						fs.write((const char *)(&min_val), 8);
// 						fs.write((const char *)(&max_val), 8);

// 						//bloom filter
// 						fs.write((const char *)bfilter.bits, 10240);

// 						for (uint64_t i = 0; i < offsetvector.size(); ++i)
// 						{
// 							fs.write((const char *)&keyvector[i], 8);
// 							fs.write((const char *)&offsetvector[i], 4);
// 						}

// 						for (uint64_t i = 0; i < offsetvector.size(); ++i)
// 						{
// 							fs.write(valuevector[i].c_str(), valuevector[i].size());
// 						}

// 						// put cache node into the cacheptr!
// 						newcache->max_val = max_val;
// 						newcache->min_val = min_val;
// 						newcache->amount = amount;
// 						newcache->timestamp = timestamp;

// 						// push in the cacheptr
// 						cacheptr[level + 1].push_back(newcache);

// 						// init the situation:
// 						amount = 0;
// 						currsize = initSize;
// 						keyvector.clear();
// 						valuevector.clear();
// 						offsetvector.clear();
// 					}
// 					// remember to delete files!
// 				}
// 				else
// 					++pointer[curr]->currpointer;
// 				// break and find next legal key
// 				break;
// 			}
// 			else
// 			{
// 				// delete from pointer if only no key left
// 				if (pointer[curr]->currpointer + 1 == pointer[curr]->amount)
// 				{
// 					for (uint64_t i = 0; i < cacheptr[pointer[curr]->level].size(); ++i)
// 						if (pointer[curr] == cacheptr[pointer[curr]->level][i])
// 						{
// 							delete pointer[curr]->stable;
// 							string deletePath = directory +
// 												cacheptr[pointer[curr]->level][i]->generate_level() + cacheptr[pointer[curr]->level][i]->generate_name();
// 							utils::rmfile(deletePath.data());
// 							cacheptr[pointer[curr]->level].erase(cacheptr[pointer[curr]->level].begin() + i);
// 							break;
// 						}
// 					delete pointer[curr]->bloomset;
// 					delete[] pointer[curr]->key_offset;
// 					delete pointer[curr];
// 					pointer.erase(pointer.begin() + curr);
// 				}
// 				else
// 				{
// 					++pointer[curr]->currpointer;
// 					++curr;
// 				}
// 			}
// 		}
// 	}
// 	// complete next
// 	compaction(level + 1);
// }

/**
 * clear the kvstore cache
 */
void KVStore::clear()
{
	// destruct timestamp & amount & cache
	for (int i = 0; i < first_level_amount; ++i)
	{
		for (int j = 0; j < cacheptr[i].size(); ++j)
		{
			delete cacheptr[i][j]->key_offset;
			delete cacheptr[i][j];
		}
	}
}
