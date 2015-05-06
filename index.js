var aws = require('aws-sdk');
var limit = require("simple-rate-limiter");
var propertiesReader = require('properties-reader');

var properties = propertiesReader('properties.file');

// properties
var AWS_ACCESS_KEY_ID = properties.get('aws.access.key');
var AWS_SECRET_ACCESS_KEY = properties.get('aws.secret.key');
var AWS_DYNAMODB_TABLE = properties.get('aws.dynamodb.table');

// set AWS configuration for future requests
aws.config.update({"accessKeyId": AWS_ACCESS_KEY_ID, "secretAccessKey": AWS_SECRET_ACCESS_KEY, "region": "eu-west-1"});
aws.config.apiVersions = {
  dynamodb: '2012-08-10'
};

var i = 1;
var dynamodb = new aws.DynamoDB();

// scan table
function scanTable(lastKey) {
	if (lastKey === null) {
		dynamodb.scan({
	        "TableName": AWS_DYNAMODB_TABLE
    	}, function (err, data) {
		    if (err) {
		    	console.log(err, err.stack);
		    	process.exit();
		    } else {
		    	processItems(data);
		    	if (data.LastEvaluatedKey != null) {
		    		scanTable(data.LastEvaluatedKey);
		    	}
		    }
		});
	} else {
		dynamodb.scan({
        	"TableName": AWS_DYNAMODB_TABLE,
        	"ExclusiveStartKey": lastKey
	    }, function (err, data) {
		    if (err) {
		    	console.log(err, err.stack);
		    	process.exit();
		    } else {
		    	processItems(data);
		    	if (data.LastEvaluatedKey != null) {
		    		scanTable(data.LastEvaluatedKey);
		    	}
		    }
		});
	}
}

// set properties to be changed
function processItems(data) {
	for (var ii in data.Items) {
        item = data.Items[ii];

        if (item.CreatedAt.S == null && item.UpdatedAt.S == null) {
        	updateApi(item.Id, item.CreatedAt.N.toString(), item.UpdatedAt.N.toString());
        } else if (item.CreatedAt.S == null || item.UpdatedAt.S == null) {
        	var createdAt, updatedAt;
	        if (item.CreatedAt.N != null) {
	        	createdAt = item.CreatedAt.N.toString();
	        } else {
	        	createdAt = item.CreatedAt.S;
	        }
	        if (item.UpdatedAt.N != null) {
	        	updatedAt = item.UpdatedAt.N.toString();
	        } else {
	        	updatedAt = item.UpdatedAt.S;
	        }
	        updateApi(item.Id, createdAt, updatedAt);
        }
	}
}

// update item
var updateApi = limit(function(id, createdAt, updatedAt) {
	dynamodb.updateItem({
    	"Key": {
    		"Id": id
    	},
	    "TableName": AWS_DYNAMODB_TABLE,
        "UpdateExpression": "SET CreatedAt = :a, UpdatedAt = :b",
	    "ExpressionAttributeValues" : {
	    	":a" : {"S":createdAt},
	    	":b" : {"S":updatedAt}
	    }
	}, function(err, data) {
	  	if (err) {
	  		console.log(err, err.stack);
		    process.exit();
	  	} else {
	  		// show progression
	  		console.log("Number of updated items:" + i);
	  		i++;
	  	}
	});
}).to(5).per(1000);

// start
scanTable(null);