var express = require('express');
var serveStatic = require('serve-static');
var compress = require('compression');
var sqlite = require('sqlite3').verbose();
var validator = require('validator');
var fs = require('fs');
var path = require('path');
var twitter = require('twitter');
var bi = require('./deps/biginteger/biginteger.js');

/**
 * Logging
 */
//TODO

/**
 * Cache of Riker tweets
 */
var dbFile = 'riker.db',
    db = new sqlite.Database(dbFile, sqlite.OPEN_READONLY);

/**
 * UI Server
 */
var server = express();
server.use(compress());
server.listen(process.env.RIKER_SERVER_PORT || 8080);

/**
 * Twitter client
 */
var client = new twitter({
  consumer_key: process.env.RIKER_TWTR_CKEY,
  consumer_secret: process.env.RIKER_TWTR_CSCRT,
  access_token_key: process.env.RIKER_TWTR_ATKN,
  access_token_secret: process.env.RIKER_TWTR_ASCRT
});

// Rebuild Riker tweets
function rebuildCache(maxId, tweets, tries, sinceId) {
  var sinceId = sinceId || "433810184817750017",
      maxId = maxId || false,
      lastMaxId = maxId,
      bigIntMaxId = false,
      tries = tries || 0,
      tweets = tweets || {};

  if(maxId) {
    bigIntMaxId = bi.BigInteger(maxId);
  }

  var queryObj = { 
    screen_name: "RikerGoogling", 
    since_id: sinceId,
    trim_user: 1,
    exclude_replies: 1,
    include_rts: 0,
    count: 200
  }

  client.get('statuses/user_timeline.json', queryObj, function(err, params, response) {
    console.log("Rebuilding", sinceId, maxId);
    if(err) {
      if(tries >= 3) {
        // Stop
        return;
      }
      return rebuildCache(maxId, tweets, tries+1, sinceId);
    }

    if(!params.length) {
      // We're done fetching
      return fillCache(tweets);
    } else {
      for(var i=0, length=params.length; i<length; i++) {
        if(sinceId == params[i].id_str) {
          // We're done
          return fillCache(tweets);
        }

        if(!params[i].text || !params[i].text.length) {
          continue;
        }

        var tweet = {
          text: params[i].text,
          char0: "",
          char1: "",
          char2: ""
        }

        if(tweet.text.length >= 1) {
          tweet.char0 = tweet.text.substring(0,1);
        }
        if(tweet.text.length >= 2) {
          tweet.char1 = tweet.text.substring(1,2);
        }
        if(tweet.text.length >= 3) {
          tweet.char2 = tweet.text.substring(2,3);
        }

        tweets[params[i].id_str] = tweet;

        if(!bigIntMaxId || bigIntMaxId.compare(params[i].id_str) > 0) {
          maxId = params[i].id_str;
        }
      }
    }

    if(maxId && maxId == lastMaxId) {
      // We're done
      return fillCache(tweets);
    } else {
      // Fetch more
      return rebuildCache(maxId, tweets, 0, sinceId);
    }
  });
};

function fillCache(tweets) {
  console.log("Filling");
  var dbRw = new sqlite.Database(dbFile);
  dbRw.run("DELETE FROM tweets", function(err) {
    if(err) {
      console.log(err);
      return;
    }

    var stmt = dbRw.prepare("INSERT INTO tweets (t_id, tweet, char0, char1, char2) VALUES($id, $tweet, $char0, $char1, $char2);")
    dbRw.serialize(function() {
      for(var i in tweets) {
        stmt.run({
          $id: i,
          $tweet: tweets[i].text,
          $char0: tweets[i].char0,
          $char1: tweets[i].char1,
          $char2: tweets[i].char2
        });
      }
    });

    stmt.finalize();
    dbRw.close();
  });
}
rebuildCache();
setInterval(rebuildCache, 86400000)

// Routes
server.get('/tweets.json', function(req, res) {
  res.header('Content-Type', 'application/javascript');
  var tweets = [];

  var query = "SELECT tweet FROM tweets";
  db.all(query, function(err, rows) {
    if(err) {
      return res.status(500).jsonp(err);
    }

    tweets = rows
    return res.jsonp(tweets);
  });
});

var oneDay = 24*60*60*1000;
server.use('/', serveStatic(path.normalize(__dirname + '/public'), { maxAge: oneDay }));