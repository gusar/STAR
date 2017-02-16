/**
 * Created by andy on 08/02/17.
 */


// Convert all postedTime fields from string to ISODate
"use star_raw"

db.msg.find().forEach(
    function(element) {
    element.object.postedTime = ISODate(element.object.postedTime);
    db.msg.save(element);
})

db.smg.createIndex( { "object.postedTime": 1 } )

db.msg.find({
    "object.postedTime": {
        $gte: ISODate("2014-11-29T00:00:00Z"),
        $lt: ISODate("2014-12-02T00:00:00.00Z")
    }
})