#pragma once

#include <assert.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <sstream> 
#include <getopt.h>
#include <limits>
#include "../dataframe/column.h"
#include "../dataframe/schema.h"
#include "../dataframe/dataframe.h"
#include "../../util/string.h"
#include "../../util/helper.h"

const int TYPE_BOOL = 0;
const int TYPE_INT = 1;
const int TYPE_FLOAT = 2;
const int TYPE_STRING = 3;

// This code is the sorer implementation of The Segfault in Our Stars team from Assignment 3
// https://github.ccs.neu.edu/xiaju/sorer < their repo link
// We only integrated it into our DataFrame, but kept base code
// Changes to original code are commented

// determine which type str belongs to
int determine_type(char* str) {
  if (strcmp(str, "0") == 0 || strcmp(str, "1") == 0 || strcmp(str, "") == 0) {
    return TYPE_BOOL;
  }
  char* endptr = NULL;
  strtol(str, &endptr, 10);
  //https://stackoverflow.com/questions/4654636/how-to-determine-if-a-string-is-a-number-with-c
  if (*endptr == 0) {
    return TYPE_INT;
  }
  //https://stackoverflow.com/questions/29169153/how-do-i-verify-a-string-is-valid-double-even-if-it-has-a-point-in-it
  auto result = float();
  auto i = std::istringstream(str);
  i >> result;      
  if (!i.fail() && i.eof()) {
    return TYPE_FLOAT;
  }
  return TYPE_STRING;
}
// convert a buffer to the given type
void* convert_to_type(int type, char* buf) {
  if (type == TYPE_BOOL) {
    if (strcmp(buf, "0") == 0) {
      return new bool(0);
    } else {
      return new bool(1);
    }
  }
  if (type == TYPE_INT) {
    return new int(atoi(buf));
  }
  if (type == TYPE_FLOAT) {
    auto result = float();
    auto i = std::istringstream(buf);
    i >> result;
    return new float(result);
  }
  if (type == TYPE_STRING) {
    size_t len = strlen(buf);
    char* dupl = new char[len + 1];
    strcpy(dupl, buf);
    return dupl;
  }
  return nullptr;
}

// converts the TYPE_BOOL, TYPE_INT, etc. into a character 'B', 'I', etc.
// ADDED - not part of original code
char change_type(int t) {
    if (t == TYPE_BOOL) return 'B';
    else if (t == TYPE_INT) return 'I';
    else if (t == TYPE_FLOAT) return 'F';
    else if (t == TYPE_STRING) return 'S';
    else error("Invalid type");
    return '\0';
}

// converts the given C++ vectors into our own Column and DataFrame types
// returns nullptr if invalid input
// ADDED - not part of original code
DataFrame* convert_to_dataframe(std::vector<int> schema, std::vector<std::vector<void*>*> columns) {
    Schema* s = new Schema();
    for (size_t i = 0; i < schema.size(); ++i) {
        s->add_column(change_type(schema[i]));
    }
    // TODO does not consider case with no columns
    for (size_t i = 0; i < columns.at(0)->size(); ++i) {
        s->add_row();
    }

    
    DataFrame* df = new DataFrame(*s);
    for (size_t c = 0; c < columns.size(); ++c) {
        char type = s->col_type(c);

        for (size_t r = 0; r < columns.at(c)->size(); ++r) {
            void* val = columns.at(c)->at(r);
            if (type == 'B') {
                bool b = 0; // default value for missing data
                if (val != nullptr) b = *(reinterpret_cast<bool*>(val));
                df->set(c, r, b);
            } else if (type == 'I') {
                int i = 0;
                if (val != nullptr) i = *(reinterpret_cast<int*>(val));
                df->set(c, r, i);
            } else if (type == 'F') {
                float f = 0;
                if (val != nullptr) f = *(reinterpret_cast<float*>(val));
                df->set(c, r, f);
            } else if (type == 'S') {
                char* cstr = reinterpret_cast<char*>(val);
                String* str;
                if (cstr == nullptr) str = new String(""); // empty string if missing
                else str = new String(cstr);
                df->set(c, r, str);
            } else error ("Invalid type");
        }
    }

    delete s;
    return df;
}

// interprets the given file into a DataFrame
// if from and len both equal 0, then the function will read the entire file
// CHANGED - this was their main function, but we removed arg parsing and some other, etc
//  - instead, we call our own convert_to_dataframe() helper on their data at the end
DataFrame* interpret_file(const char* filename, size_t from, size_t len) {
  if (from == 0 && len == 0) {
    from = 0;
    len = std::numeric_limits<unsigned int>::max();
  }

  char buf[256];
  size_t ind = 0;
  bool ignore_spaces = true;


  std::ifstream ifs;
  ifs.open(filename, std::ifstream::in);

  // parse the first 500 lines to determine schema
  std::vector<char*>* cur_vector = new std::vector<char*>();
  std::vector<int> data_types;
  size_t cur_col = 0;
  size_t max_col = 0;
  size_t cur_row = 0;
  // TODO remove size_t max_row;

  char c = ifs.get();
  while (ifs.good() && cur_row <= 500) {
    switch(c) {
      case '<' :
        ind = 0;
        break;
      case '>' :
        {
          buf[ind] = '\0';
          size_t len = strlen(buf);
          char* dupl = new char[len + 1];
          strcpy(dupl, buf);
          cur_vector->push_back(dupl);

          int t = determine_type(dupl);
          if (cur_col >= data_types.size()) {
            data_types.push_back(t);
          } else if (t > data_types[cur_col]) {
            data_types[cur_col] = t;
          }
          cur_col++;
          ind = 0;
          delete[] dupl;
          break;
        }
      case ' ' :
        if (!ignore_spaces) {
          buf[ind] = c;
          ind++;
        }
        break;
      case '\"' :
        ignore_spaces = !ignore_spaces;
        break;
      case '\n' :
        if (cur_col - 1 > max_col && cur_col != 0) {
          max_col = cur_col - 1;
          //TODO remove max_row = cur_row;
        }
        delete cur_vector;
        cur_vector = new std::vector<char*>();
        cur_col = 0;
        cur_row++;
        break;
      default :
        buf[ind] = c;
        ind++;
    }
    c = ifs.get();
  }
  delete cur_vector;
  ifs.close();
  ifs.clear();

  ifs.open(filename, std::ifstream::in);
  
  // actually read file according to given parameters
  std::vector<std::vector<void*>*> columns;
  cur_col = 0;
  cur_row = 0;
  ind = 0;
  ignore_spaces = true;

  for (size_t i = 0; i <= max_col; i++) {
    columns.push_back(new std::vector<void*>());
  }

  if (from != 0) {
    ifs.ignore(from); 
    ifs.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }

  c = ifs.get();
  size_t bytes_read = 1;

  while (ifs.good() && (bytes_read < len)) {
    switch(c) {
      case '<' :
        ind = 0;
        break;
      case '>' :
        {
          buf[ind] = '\0';
          if (strlen(buf) != 0) {
            columns[cur_col]->push_back(convert_to_type(data_types[cur_col], buf));
          } else {
            columns[cur_col]->push_back(NULL);
          }

          cur_col++;
          ind = 0;
          break;
        }
      case ' ' :
        if (!ignore_spaces) {
          buf[ind] = c;
          ind++;
        }
        break;
      case '\"' :
        ignore_spaces = !ignore_spaces;
        break;
      case '\n' :
        // fill a column until it reaches the max column length
        for (size_t i = cur_col; i <= max_col; i++) {
          columns[i]->push_back(NULL);
        }
        cur_col = 0;
        cur_row++;
        break;
      default :
        buf[ind] = c;
        ind++;
    }
    c = ifs.get();
    bytes_read++;
  }

  // delete any incomplete rows
  if (ifs.good()) {
    for (size_t i = 0; i < columns.size(); i++) {
      if (columns[i]->size() == cur_row - 1) {
        columns[i]->pop_back();
     }
    }
  }

  DataFrame* out = convert_to_dataframe(data_types, columns);
  for (size_t i = 0; i < columns.size(); ++i) {
      for (size_t j = 0; j < columns[i]->size(); ++j) {
        if (data_types[i] == TYPE_BOOL) delete reinterpret_cast<bool*>(columns[i]->at(j));
        else if (data_types[i] == TYPE_INT) delete reinterpret_cast<int*>(columns[i]->at(j));
        else if (data_types[i] == TYPE_FLOAT) delete reinterpret_cast<float*>(columns[i]->at(j));
        else if (data_types[i] == TYPE_STRING) delete[] reinterpret_cast<char*>(columns[i]->at(j));
      }
      delete columns[i];
  }
  return out;
}


