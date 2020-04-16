// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)
// API provided in Assignment 4

// lang::CwC

#pragma once

#include "../../util/object.h"
#include "../../util/helper.h"

/* Fielder::
 * A field vistor invoked by Row.
 * This class should be overwritten since it is subclassed.
 */
class Fielder : public Object {
    public:
 
        /** Called before visiting a row, the argument is the row offset in the
        dataframe. */
        virtual void start(size_t r) {check(false, "Called in parent class");}
 
        /** Called for fields of the argument's type with the value of the field. */
        virtual void accept(bool b) {check(false, "Called in parent class");}
        virtual void accept(float f) {check(false, "Called in parent class");}
        virtual void accept(int i) {check(false, "Called in parent class");}
        virtual void accept(String* s) {check(false, "Called in parent class");}
 
        /** Called when all fields have been seen. */
        virtual void done() {check(false, "Called in parent class");}
};
