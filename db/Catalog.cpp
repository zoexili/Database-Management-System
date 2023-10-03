#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    // TODO pa1.2: implement
    int tableID = file->getId();

    // Identify if there's an existing table with the same name but different ID, keep the 2nd one.
    int idToRemove = -1;
    for (const auto &key: names) {
        if (key.second == name) {
            if (key.first != tableID) {
                idToRemove = key.first;
                break;
            }
        }
    }

    // If there's a conflicting table, remove its entries
    if (idToRemove != -1) {
        names.erase(idToRemove);
        dbFiles.erase(idToRemove);
        pkeyFields.erase(idToRemove);
    }

    // Insert or update the new entries
    names[tableID] = name;
    dbFiles[tableID] = file;
    pkeyFields[tableID] = pkeyField;
}

int Catalog::getTableId(const std::string &name) const {
    // TODO pa1.2: implement
    for (auto &key: names) {
        if (key.second == name) {
            return key.first;
        }
    }
    throw std::invalid_argument("This table name does not exist");
}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
    // TODO pa1.2: implement
    if (dbFiles.count(tableid) > 0) {
        return dbFiles.find(tableid)->second->getTupleDesc();
    }
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    // TODO pa1.2: implement
    return dbFiles.find(tableid)->second;
}

std::string Catalog::getPrimaryKey(int tableid) const {
    // TODO pa1.2: implement
    return pkeyFields.find(tableid)->second;
}

std::string Catalog::getTableName(int id) const {
    // TODO pa1.2: implement
    return names.find(id)->second;
}

void Catalog::clear() {
    // TODO pa1.2: implement
    names.clear();
    dbFiles.clear();
    pkeyFields.clear();
}
