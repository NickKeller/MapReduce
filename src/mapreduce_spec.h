#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <cstdlib>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	//the full path of the config file that generated this spec
	std::string config_filename;
	//number of workers
	int num_workers;
	//vector of worker ip addresses
	std::vector<std::string> worker_ipaddr_ports;
	//vector of the input files
	std::vector<std::string> input_files;
	//the directory to output files to
	std::string output_dir;
	//the number of output files to create
	int num_output_files;
	//the number of kilobytes in each shard
	int map_kilobytes;
	//the user id
	std::string user_id;
};


//helper methods for splitting a string
inline void split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
}


inline std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	//assuming that the config file is a valid file
	std::ifstream config_file(config_filename);
	if(config_file.good()){
		mr_spec.config_filename = config_filename;
		std::vector<std::string> lines;
		std::copy(std::istream_iterator<std::string>(config_file),
          std::istream_iterator<std::string>(),
          std::back_inserter(lines));

		for(auto line : lines){
			std::vector<std::string> keyValue = split(line,'=');
			std::string key = keyValue.at(0);
			std::string value = keyValue.at(1);
			//switch on the value
			if(key.compare("n_workers") == 0){
				mr_spec.num_workers = atoi(value.c_str());
			}
			if(key.compare("worker_ipaddr_ports") == 0){
				mr_spec.worker_ipaddr_ports = split(value,',');
			}
			if(key.compare("input_files") == 0){
				mr_spec.input_files = split(value,',');
			}
			if(key.compare("output_dir") == 0){
				mr_spec.output_dir = value;
			}
			if(key.compare("n_output_files") == 0){
				mr_spec.num_output_files = atoi(value.c_str());
			}
			if(key.compare("map_kilobytes") == 0){
				mr_spec.map_kilobytes = atoi(value.c_str());
			}
			if(key.compare("user_id") == 0){
				mr_spec.user_id = value;
			}
		}
		return true;
	}
	else{
		std::cout << "Bad config file: " << config_filename << std::endl;
		return false;
	}
	return false;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	//for now, print out all of the members
	std::cout << "Config file: " << mr_spec.config_filename << std::endl;
	std::cout << "Number of Workers: " << mr_spec.num_workers << std::endl;
	std::cout << "Worker IP Addresses: " << std::endl;
	for(auto worker : mr_spec.worker_ipaddr_ports){
		std::cout << "\t" << worker << std::endl;
	}
	std::cout << "Input Files: " << std::endl;
	for(auto input : mr_spec.input_files){
		std::cout << "\t" << input << std::endl;
	}
	std::cout << "Output Dir: " << mr_spec.output_dir << std::endl;
	std::cout << "Number of output files: " << mr_spec.num_output_files << std::endl;
	std::cout << "Number of kilobytes in each shard: " << mr_spec.map_kilobytes << std::endl;
	std::cout << "User ID: " << mr_spec.user_id << std::endl;

	//make sure that num_workers and the number of workers given matches
	if(mr_spec.num_workers != mr_spec.worker_ipaddr_ports.size()){
		std::cout << "ERROR!! Number of workers doesn't match." << std::endl;
		std::cout << "Number of workers given: " << mr_spec.num_workers << std::endl;
		std::cout << "Number or worker ip addresses given: " << mr_spec.worker_ipaddr_ports.size() << std::endl;
		return false;
	}

	//make sure that the input files exist
	for(auto input_file : mr_spec.input_files){
		std::ifstream test(input_file.c_str());
		if(!test.good()){
			std::cout << "ERROR!! Input file: " << input_file << " doesn't exist!" << std::endl;
			return false;
		}
	}

	return true;
}
