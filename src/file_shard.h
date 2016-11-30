#pragma once

#include <vector>
#include <math.h>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct file_key_offset{
	std::string filename;

	//specifies an offset within a file
	//[startOffset,endOffset)
	int startOffset;
	int endOffset;

};
struct FileShard {
	int shard_id;
	std::vector<file_key_offset*> pieces;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	std::cout << "File shard: enter" << std::endl;
	std::vector<std::string> filenames = mr_spec.input_files;
	//figure out how to shard the files.
	//first, sum up the file sizes
	int totalsize;
	FileShard* shard = new FileShard;
	int bytes_used = 0;
	int shard_id = 0;
	for(auto filename : filenames){
		std::cout << "New file: " << filename << std::endl;
		std::ifstream file(filename,std::ifstream::ate | std::ifstream::binary);
		file_key_offset* piece = new file_key_offset;
		//get the size of the file, in kilobytes
		file.seekg(0,file.end);
		int size = file.tellg();
		file.seekg(0,file.beg);
		//time to iterate through the file
		int cur_offset = 0;
		char ch;
		while(!file.eof()){
			//undoes the offset that is done at the end of this loop.
			file.seekg(cur_offset);
			//seek to map_kilobytes or eof, whichever is first and has room
			//how many kb left?
			int num_bytes_left = (mr_spec.map_kilobytes*1000 - bytes_used);
			int size_left = (size) - cur_offset;
			std::cout << "Bytes left: " << num_bytes_left << ", size left: " << size_left << std::endl;
			int num_to_seek = fmin(num_bytes_left,size_left);
			std::cout << "Seeking " << num_to_seek << " bytes" << std::endl;
			file.seekg(cur_offset + num_to_seek);
			piece->filename = filename;
			piece->startOffset = cur_offset;
			//before we set the end offset,
			//find the next newline character from this point
			//only do it if we didn't seek to the end of file
			if(num_to_seek != size_left){
				std::cout << "Finding next newline character" << std::endl;
				char c;
				int nl_offset = 0;
				while(c != '\n'){
					file.get(c);
					nl_offset++;
				}
				std::cout << "Found newline character" << std::endl;
				std::cout << "Character: " << c << "BLAH" << std::endl;
				cur_offset += nl_offset;
			}
			piece->endOffset = file.tellg();
			std::cout << "start: " << piece->startOffset << " end: " << piece->endOffset << std::endl;
			//push the piece back onto the shard
			shard->pieces.push_back(piece);
			cur_offset += num_to_seek;
			piece = new file_key_offset;
			bytes_used += num_to_seek;
			if(bytes_used >= mr_spec.map_kilobytes*1000){
				//set the shard id
				shard->shard_id =  shard_id;
				fileShards.push_back(*shard);
				std::cout << "Shard done, id: " << shard->shard_id << std::endl;
				shard = new FileShard;
				shard_id++;
				bytes_used = 0;
			}
			//do this so that if we are at the end of a file, it will try to read one more bit
			//have to do this because seekg clears the eofbit by default at the start of the function
			file >> ch;
		}
		//no matter what, at the end of a file, push back
		shard->pieces.push_back(piece);
		totalsize += size;
		std::cout << "File: " << filename << " Size: " << size << "B" << std::endl;
	}
	shard->shard_id = shard_id;
	fileShards.push_back(*shard);

	//got the total size, now to figure out the number of shards

	auto num_shards = ceil(totalsize/(mr_spec.map_kilobytes*1000));
	std::cout << "Total Size: " << totalsize << ", Number of shards: " << num_shards << std::endl;
	std::cout << "Actual number of shards: " << fileShards.size() << std::endl;

	return true;
}
