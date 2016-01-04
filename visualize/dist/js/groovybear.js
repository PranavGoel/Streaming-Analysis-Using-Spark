// jQuery.noConflict();
(function($) {
    $(function() {
        // more code using $ as alias to jQuery
        $(document).ready(function() {
            // Function to retain only unique values in an array
            function uniques(arr) {
                var a = [];
                for (var i=0, l=arr.length; i<l; i++)
                    if (a.indexOf(arr[i]) === -1 && arr[i] !== '')
                        a.push(arr[i]);
                return a;
            }
            
            
            // Function to add opacity to given color
            function convertHex(hex,opacity){
                hex = hex.replace('#','');
                r = parseInt(hex.substring(0,2), 16);
                g = parseInt(hex.substring(2,4), 16);
                b = parseInt(hex.substring(4,6), 16);

                result = 'rgba('+r+','+g+','+b+','+opacity/100+')';
                return result;
            }
            
            // Function to generate random colors
            function randomColor(brightness){
              function randomChannel(brightness){
                var r = 255-brightness;
                var n = 0|((Math.random() * r) + brightness);
                var s = n.toString(16);
                return (s.length==1) ? '0'+s : s;
              }
              return '#' + randomChannel(brightness) + randomChannel(brightness) + randomChannel(brightness);
            }
        
            
            // Big bad lookup object
            data = {}
            
            // Reference:
            // http://stackoverflow.com/questions/5484673/javascript-how-to-dynamically-create-nested-objects-using-object-names-given-by/11433067#11433067
            var createNestedObject = function( base, names, value ) {
                // If a value is given, remove the last name and keep it for later:
                var lastName = arguments.length === 3 ? names.pop() : false;

                // Walk the hierarchy, creating new objects where needed.
                // If the lastName was removed, then the last object is not set yet:
                for( var i = 0; i < names.length; i++ ) {
                    base = base[ names[i] ] = base[ names[i] ] || {};
                }

                // If a value was given, set it to the last name:
                if( lastName ) base = base[ lastName ] = value;

                // Return the last object in the hierarchy:
                return base;
            };
            
            var response = $.ajax({
                url: 'http://www2.cs.sfu.ca/people/GradStudents/sood/personal/output/summary/part-00000',
                beforeSend: function (request){
                    request.setRequestHeader("Access-Control-Allow-Origin", "*");
                    request.setRequestHeader("Access-Control-Allow-Credentials", true);
                    request.setRequestHeader("Access-Control-Allow-Methods", "OPTIONS, GET, POST");
                    request.setRequestHeader("Access-Control-Allow-Headers", "Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control");                    
                },
                success: function(result){
                    $('#textdata').html(result);
                    return result;
                },
                global: false,
                async: false
            }).responseText.trim();
            
            // Set of all date time labels, genres and respective counts
            var labels  = [];
            var genres  = [];
            var counts   = [];
            
            // Set of unique date time labels and genres
            var labels_set = [];
            var genres_set = [];
            
            // Split response text at each new line character
            var items = String(response).split(/\r?\n/);
            
            // Get all date time labels, genres and respective counts
            for( item in items ){
                labels.push(items[item].split(",")[0]);
                genres.push(items[item].split(",")[1]);
                counts.push(items[item].split(",")[2]);
                
                // Insert into data: Genre | Date | Count
                createNestedObject(data, [items[item].split(",")[1], items[item].split(",")[0]], parseInt(items[item].split(",")[2]) );
            }
            
            // Create unique data time labels and genres in the dataset
            labels_set = uniques(labels);
            genres_set = uniques(genres);
            
            // Generate datasets from what we have
            all_datasets = []
            
            
            for( genre in genres_set) {
            
                // Prepare genre dataset attributes for charting
                attrib = {}
            
                fill_color = convertHex(randomColor(Math.random() * 250), 40);
                stroke_color = convertHex(randomColor(Math.random() * 200), 100);
            
                createNestedObject(attrib, ['label'], genres_set[genre]);
                createNestedObject(attrib, ['fillColor'], fill_color);
                createNestedObject(attrib, ['strokeColor'], stroke_color);
                createNestedObject(attrib, ['pointColor'], stroke_color);
                createNestedObject(attrib, ['pointStrokeColor'], stroke_color);
                createNestedObject(attrib, ['pointHighlightFill'], stroke_color);
                createNestedObject(attrib, ['pointHighlightStroke'], stroke_color);
                
                // Empty list of data for this genre
                createNestedObject(attrib, ['data'], []);
            
                for( label in labels_set) {
                    // Handle missing values, as some genres might not be tweeted about at all in a given hour
                    if(! data[genres_set[genre]][labels_set[label]]) {
                        createNestedObject(data, [genres_set[genre], labels_set[label]], 0);
                    }
                    
                    // Populate data attribute
                    attrib['data'].push(data[genres_set[genre]][labels_set[label]])
                }
                
                // Prepare the final datasets
                all_datasets.push(attrib);
            }


            var lineChartData = {
                labels: labels_set,
                datasets: all_datasets
            }
            
            var ctx = document.getElementById("canvas").getContext("2d");
            window.myLine = new Chart(ctx).Line(lineChartData, {
                responsive: true
            });
            
            var legendHolder = document.createElement('div');
            legendHolder.innerHTML = window.myLine.generateLegend();

            document.getElementById('legend').appendChild(legendHolder.firstChild);
            
        });
    });
})(jQuery);
