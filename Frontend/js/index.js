AWS.config.update({
    accessKeyId: "AKIA5YFPVFQIDOSLB6GP",
    secretAccessKey: "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",
    region: 'us-east-1'
});

function load(){

	var allKeys = [];

	var apigClient = apigClientFactory.newClient({
    accessKey: 'AKIA5YFPVFQIKEW2DCCG',
    secretKey: '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'
	});

	var params = {

	};

	var body = {
	  // This is where you define the body of the request,
	};

	var additionalParams = {

	};

	var allKeys = []
	var allTitle = []

	apigClient.rootGet(params, body, additionalParams)
	    .then(function(result){
	      // Add success callback code here.
	      
	      console.log(result);
	      result.data.forEach(function(item){
	      	allKeys.push(item);
	      });
	      updateThumbnails(allKeys)
	    }).catch( function(result){
	      console.log(result);
	    });
}

function upload(){
	var file = document.getElementById("upload").files[0]
	/* var name = $('#name').val().trim();
	if(name.length == 0){
		name = file.name;
	} */

	// console.log(name)
	// const fileContent = fs.readFileSync(file);
	const params = {
		Bucket: "project-temp-bucket",
		Key: file.name,
		Body: file,
		Metadata:{
			'customlabels': 'video-ui-upload'
		}
	}

	document.getElementById("uploadform").style.display = "none";
	document.getElementById("upload").value = "";
	$('#name').val("");
	document.getElementById("uploadSubmit").disabled = true;
	console.log("upload successful");
	
	s3.upload(params, function(err, data) {
		if(err){
			throw err;
		}
	})
}