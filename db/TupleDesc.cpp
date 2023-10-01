#include <db/TupleDesc.h>
#include <algorithm>
#include <iostream>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    // TODO pa1.1: implement
    return fieldType == other.fieldType && fieldName == other.fieldName;
}

// calculate TDItem's hash value and return it
std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    // TODO pa1.1: implement
    std::size_t typeHash = std::hash<Types::Type>()(r.fieldType);
    std::size_t nameHash = std::hash<std::string>()(r.fieldName);
    return typeHash ^ (nameHash << 1); 
}

//
// TupleDesc
//

// TODO pa1.1: implement
// initialize your private member variables with the provided arguments
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    TDTypes = types;
    for (size_t i = 0; i < TDTypes.size(); i++) {
        TDItemVec.emplace_back(TDTypes[i], "");
    }
}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    if (types.size() != names.size()) {
        throw std::invalid_argument("Types and Names must have the same size.");
    }
    TDTypes = types;
    TDNames = names;
    for (size_t i = 0; i < TDTypes.size(); i++) {
        TDItemVec.emplace_back(TDTypes[i], TDNames[i]);
    }
}

size_t TupleDesc::numFields() const {
    // TODO pa1.1: implement
    return TDTypes.size();
}

std::string TupleDesc::getFieldName(size_t i) const {
    // TODO pa1.1: implement
    // if (i >= TDNames.size()) {
    //     throw std::out_of_range("Index out of bound for TD Names.");
    // }
    // return TDNames[i];
    return (i < TDNames.size()) ? TDNames[i] : throw std::out_of_range("Index out of bound for TDNames.");
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    // TODO pa1.1: implement
    // if (i >= TDTypes.size()) {
    //     throw std::out_of_range("Index out of bound for TDTypes.");
    // }
    // return TDTypes[i];
    return (i < TDTypes.size()) ? TDTypes[i] : throw std::out_of_range("Index out of bound for TDTypes.");
}


// Find the field index based on field name
int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    // TODO pa1.1: implement
    auto it = std::find(TDNames.begin(), TDNames.end(), fieldName);

    if (it != TDNames.end()) {
        // Calculate the index
        int index = std::distance(TDNames.begin(), it);
        return index;
    }
    throw std::invalid_argument("Unknown Field Name");
}

// A field has its type and its name, you need to sum up the bits of each field's type
size_t TupleDesc::getSize() const {
    // TODO pa1.1: implement
    int total_sum = 0;
    if (TDTypes.size() > 0) {
        for (auto &TDType : TDTypes) {
            total_sum += Types::getLen(TDType);
        }
    }
    return total_sum;
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1.1: implement
    TupleDesc merge_TD;
    merge_TD.TDTypes.resize(td1.numFields() + td2.numFields());
    merge_TD.TDNames.resize(td1.numFields() + td2.numFields());
    int idx = 0;
    for (int i = 0; i < td1.numFields(); i++) {
        merge_TD.TDTypes[idx] = td1.TDTypes[i];
        merge_TD.TDNames[idx++] = td1.TDNames[i];
    }
    for (int i = 0; i < td2.numFields(); i++) {
        merge_TD.TDTypes[idx] = td2.TDTypes[i];
        merge_TD.TDNames[idx++] = td2.TDNames[i];
    }
    return merge_TD;
}

// return a string: "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])"
std::string TupleDesc::to_string() const {
    // TODO pa1.1: implement
    std::string str;
    for (auto it = TDItemVec.begin(); it != TDItemVec.end(); it++) {
        int idx = fieldNameToIndex(it->fieldName);
        str += Types::to_string(it->fieldType);
        str += "[";
        str += idx;
        str += "](";
        str += it->fieldName;
        str += "[";
        str += idx;
        str += "])";
        if (std::next(it) != TDItemVec.end()) {
            str += ", ";
        }
    }
    return str;
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    // TODO pa1.1: implement
    if (TDNames.size() != other.TDNames.size()) {
        return false;
    }
    if (TDTypes.size() != other.TDTypes.size()) {
        return false;
    }
    for (int i = 0; i < TDTypes.size(); i++) {
        // cannot check if a string is null in cpp
        if (getFieldName(i) != (other.getFieldName(i))) {
            return false;
        }
        else if (getFieldType(i) != (other.getFieldType(i))) {
            return false;
        }
    }
    return true;
}

TupleDesc::iterator TupleDesc::begin() const {
    // TODO pa1.1: implement
    return TDItemVec.begin();
}

TupleDesc::iterator TupleDesc::end() const {
    // TODO pa1.1: implement
    return TDItemVec.end();
}

// calculate TupleDesc's hash value and return it
std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    // TODO pa1.1: implement
    size_t hashVal = 0;
    for (auto &TDItem : td) {
        std::hash<db::TDItem> itemhasher;
        size_t tdhash = itemhasher(TDItem);
        hashVal ^= (tdhash << 1);
    }
    return hashVal;
}
