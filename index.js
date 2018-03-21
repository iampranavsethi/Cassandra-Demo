var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var client = new cassandra.Client({contactPoints: ['127.0.0.1'], keyspace: 'hw4'});

var multer  = require('multer');
var str = multer.memoryStorage();
var upload = multer({ storage: str });

client.connect(function(err, result){
  if (err)
    console.log("E");
  else console.log("Connected!");
});

app.use(bodyParser.json());

app.post('/deposit', upload.single('contents'), function(req, res, next){
  console.log("/deposit :" + req.body.filename);
  console.log("/deposit :" + req.file.fieldname + " # " + req.file.originalname);
  var query = "INSERT INTO imgs (filename, content) VALUES (?, ?)";
  var p = [ req.body.filename, req.file.buffer ];

  client.execute(query, p, {prepare: true} , function(err, result){
    if (err)
      console.log("ERROR " + err);
    else{
      console.log('Filename %s inserted', result);
      res.send("OK");
    }
  });
});

app.get('/retrieve', function(req, res){
  console.log("/retrieve: " + req.query.filename);
  var query = "SELECT * FROM imgs WHERE filename = ?";
  client.execute(query, [ req.query.filename], function(err, result){
    if (err)
      console.log("ERROR");
    else{
      console.log('Filename %s required!', result.rows[0].filename);
      res.contentType(result.rows[0].filename);
      res.send(result.rows[0].content);
    }
  });

});

app.listen(80, function () {
  console.log('Example app listening on port 80!');
});
