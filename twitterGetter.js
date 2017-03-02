var Twitter = require('node-twitter');
var fs = require("fs");
var jsonminify = require("jsonminify");

var twitterStreamClient = new Twitter.StreamClient(
  'CONSUMER_KEY',
  'CONSUMER_SECRET',
  'TOKEN',
  'TOKEN_SECRET'
);

twitterStreamClient.on('close', function() {
    console.log('Connection closed.');
});
twitterStreamClient.on('end', function() {
    console.log('End of Line.');
    gatherData();
});
twitterStreamClient.on('error', function(error) {
    console.log('Error: ' + (error.code ? error.code + ' ' + error.message : error.message));
});
twitterStreamClient.on('tweet', function(tweet) {
    var date = tweet.created_at.split(' ');

    fs.appendFile('data/' + date[1]+date[2] + '.txt', JSON.stringify(tweet) + '\n', function (err) {
      console.log("couldn't save tweet " + tweet.id);
    });
    //console.log("got tweet " + tweet.id);
});

function gatherData() {
  twitterStreamClient.start(['#hillary', '#trump', '#2016election', '#donaldtrump', '#hillaryclinton', 'Trump', 'Hillary']);
};

gatherData();