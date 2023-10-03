#include <db/TupleDesc.h>
#include <algorithm>
#include <iostream>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    // TODO pa1.1: implement
    if (this == &other) {
        return true;
    }
    if (this->fieldType == other.fieldType) {
        return true;
    }
    return false;
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

TupleDesc::TupleDesc(std::vector<TDItem> &TDItemVec){
    this->TDItemVec=TDItemVec;
    this->fieldTot=TDItemVec.size();
}

// TODO pa1.1: implement
// initialize your private member variables with the provided arguments
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    fieldTot = types.size();
    for(int i = 0; i < fieldTot; i++) {
        TDItemVec.emplace_back(types[i], "");
    }

}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    fieldTot = types.size();

    for(int i=0; i<fieldTot; i++){
       TDItemVec.emplace_back(types[i], names[i]);
   }
}

size_t TupleDesc::numFields() const {
    // TODO pa1.1: implement
    return this->fieldTot;
}

std::string TupleDesc::getFieldName(size_t i) const {
    // TODO pa1.1: implement
    return TDItemVec[i].fieldName;
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    // TODO pa1.1: implement
    return TDItemVec[i].fieldType;
}


// Find the field index based on field name
int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    // TODO pa1.1: implement
    if(fieldName.empty()){
        throw std::invalid_argument("");
    }

    for (int i = 0; i < TDItemVec.size(); i++)
    {
        if (TDItemVec[i].fieldName==fieldName)
        {
            return i;
        }
    }
    throw std::invalid_argument("");
}

// A field has its type and its name, you need to sum up the bits of each field's type
size_t TupleDesc::getSize() const {
    // TODO pa1.1: implement
    unsigned long tot=0;
    for (const TDItem& item : this->TDItemVec) {
        tot += getLen(item.fieldType);
    }
    return tot;
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1.1: implement
    std::vector<TDItem> mergedTD = td1.TDItemVec;
    mergedTD.insert(mergedTD.end(), td2.TDItemVec.begin(), td2.TDItemVec.end());

    return TupleDesc(mergedTD);
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
    bool res = true;
    if(this==&other){
        return true;
    }
    if(this->numFields()!=other.numFields()){
        return false;
    }
    for (int i = 0; i < numFields(); i++)
    {
        if(this->TDItemVec[i]!=other.TDItemVec[i])
        {
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
