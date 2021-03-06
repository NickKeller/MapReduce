#pragma once

#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <utility>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

    /* DON'T change this function's signature */
    BaseMapperInternal();

    /* DON'T change this function's signature */
    void emit(const std::string& key, const std::string& val);

    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    void write_data(std::string name, int R);
    // map is already sorted
    std::map<std::string, std::vector<std::string>> pairs;
    std::vector<std::string>file_names;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;

    // Normal case, when the map already contains the key
    // this builds the vector of vals for each key
    try{
        pairs.at(key).push_back(val);
    }
    // Case: first insertion of a key, catch the exepction and construct things
    // using c++11 semantics, not c++17
    // using map's emplace
    // using vectors initilaization list
    catch(std::out_of_range const& e){
        std::vector<std::string> vec {val};
        pairs.emplace(key, vec);
    }
    // the above structure looks like:
    // {key1:[val1,val2,val3,val4],key2:[val1,val2],key3:[val1]}
    // hopefully this doesn't get too big for the machines memory.....
}
inline void BaseMapperInternal::write_data(std::string name, int R){
    // loop and open/create files for each of the output files
    // iterate through the map, hash the key, write it to the appropriate file
    std::hash<std::string> hash_fn;
    for(int i = 0; i < R; i++){
        file_names.push_back(name + "_R" + std::to_string(i));
    }
    // iterate through map, hashing and writing thusly:
    // key1:val1,val2,val3,val4
    // key2:val1,val2
    // key3:val1
    for(auto iter : pairs){
        int index = (hash_fn(iter.first) % R);
        //opening an ofstream, automatically puts it in write, adding append to get to the end
        std::ofstream inter_file(file_names[index],std::ios::app);
        // add the first item of the vector
        inter_file << iter.first << ':' << iter.second.front();
        //starting at 1, for proper comma separation
        for(int vec_iter = 1; vec_iter < iter.second.size(); vec_iter++ ){
            inter_file << ',' << vec_iter;
        }
        inter_file << std::endl;
    }
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
        void group_keys();

        std::string final_file;
        std::vector<std::string> temp_files;
        std::map<std::string, std::vector<std::string>> pairs;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    // open filename for appending
    std::ofstream out(final_file, std::ios::app);

	out <<  key << ", " << val << std::endl;
}

inline void BaseReducerInternal::group_keys(){
    // parse temp_files into pairs structure
    for(auto file_name : temp_files){
        std::ifstream temp_file(file_name);
        for(std::string line; std::getline(temp_file,line);){

            std::istringstream parse(line);
            std::string key;
            std::getline(parse, key, ':');

            // if the key isn't in the map, add it
            auto missing = pairs.find(key);
            if( missing == pairs.end()){
                std::vector<std::string> vec;
                pairs.emplace(key,vec);
            }
            //push_back vals to the vector
            for(std::string val; std::getline(parse,val,',');){
                pairs.at(key).push_back(val);
            }

        }//for line in file
    }//for file in files
}
