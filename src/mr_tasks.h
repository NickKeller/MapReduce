#pragma once

#include <string>
#include <iostream>
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

        // map is already sorted
        std::map<std::string, std::vector<std::string>> pairs;
        // Logan - hash the emitted keys mod number of workers??
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
    // {key1:[val1,val2,val3,val4],key2:[val1,val2],key3:[key1]}
    //
    // hopefully this doesn't get too big for the machines memory.....
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
        
        std::string emit_file_name;
        // Logan - make a member for the filename
        // Logan - sort the keys
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    // append key and value to io stream
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
