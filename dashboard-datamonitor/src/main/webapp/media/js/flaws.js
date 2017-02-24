//var editor; // use a global for the submit and return data rendering in the contact
var otable;
var dataTab;

var textit_prefix = "./api/zambia";

(function (factory) {
    if (typeof define === "function" && define.amd) {
        define(["jquery", "moment", "datatables.net"], factory);
    } else {
        factory(jQuery, moment);
    }
}(function ($, moment) {
 
$.fn.dataTable.moment = function ( format, locale ) {
    var types = $.fn.dataTable.ext.type;
 
    // Add type detection
    types.detect.unshift( function ( d ) {
        if ( d ) {
            // Strip HTML tags and newline characters if possible
            if ( d.replace ) {
                d = d.replace(/(<.*?>)|(\r?\n|\r)/g, '');
            }
 
            // Strip out surrounding white space
            d = $.trim( d );
        }
 
        // Null and empty values are acceptable
        if ( d === '' || d === null ) {
            return 'moment-'+format;
        }
 
        return moment( d, format, locale, true ).isValid() ?
            'moment-'+format :
            null;
    } );
 
    // Add sorting method - use an integer for the sorting
    types.order[ 'moment-'+format+'-pre' ] = function ( d ) {
        if ( d ) {
            // Strip HTML tags and newline characters if possible
            if ( d.replace ) {
                d = d.replace(/(<.*?>)|(\r?\n|\r)/g, '');
            }
 
            // Strip out surrounding white space
            d = $.trim( d );
        }
         
        return d === '' || d === null ?
            -Infinity :
            parseInt( moment( d, format, locale, true ).format( 'x' ), 10 );
    };
};
 
}));


$(document).ready(function() {
	$.fn.dataTable.ext.errMode = 'none';
	
    editor = new $.fn.dataTable.Editor( {
        table: "#flow",
        fields: [ {
                label: "Flow UUID:",
                name: "uuid",
				type: "readonly"
            }, {
                label: "Flow Name:",
                name: "name",
				type: "readonly"
            }, {
                label: "Total Runs:",
                name: "runs",
				type: "readonly"
            }, {
                label: "Completed Runs:",
                name: "completed_runs",
				type: "readonly"
            }, {
                label: "Created Date:",
                name: "created_on",
				type: "readonly"
            }, {
                label: "Country:",
                name: "country",
				type: "readonly"
            }, {
                label: "Season:",
                name: "season",
				fieldInfo: "Ex: Planting, Growing, Harvesting, Inter-Season"
            }, {
                label: "Creator:",
                name: "creator",
				fieldInfo: "Ex: Jacob"
            }, {
                label: "Flow Type:",
                name: "flow_type",
				fieldInfo: "Ex: Regular, Test, Deprecated, Archived"
            }, {
                label: 'Run Start Date:',
				name: 'run_start_date',
                type: 'datetime',
                format: 'YYYY-MM-DD HH:mm:ss',
                fieldInfo: 'EX: 03-10-2016 2.30 PM'
            }, {
                label: 'Run End Date:',
                name: 'run_end_date',
                type: 'datetime',
                format: "YYYY-MM-DD HH:mm:ss",
                fieldInfo: 'EX: 09-10-2016 11.55 PM'
            }
        ]
    } );
	
	
	$('#flow').on( 'click', 'tbody td:not(:first-child)', function (e) {
        editor.inline( this, {
            buttons: { label: '&gt;', fn: function () { 
			
			var table = $('#flow').DataTable();
			
			$('div .DTE_Form_Buttons').click(function () {			
				
				var season = $("#DTE_Field_season").val();
				var creator = $("#DTE_Field_creator").val();
				var flow_type = $("#DTE_Field_flow_type").val();
				var run_start_date_full = $("#DTE_Field_run_start_date").val();				
				var run_end_date_full = $("#DTE_Field_run_end_date").val();				
				var uuid = $(this).closest('tr').find('td:eq(1)').text();
				
				if(season==undefined){
					season = $(this).closest('tr').find('td:eq(4)').text();
				}if(creator==undefined){
					creator = $(this).closest('tr').find('td:eq(5)').text();
				}if(flow_type==undefined){
					flow_type = $(this).closest('tr').find('td:eq(6)').text();
				}if(run_start_date_full==undefined){
					run_start_date_full = $(this).closest('tr').find('td:eq(7)').text();
					
					if(run_start_date_full==""){
					run_start_date = "";
					run_start_time = "";
					}else{
					run_start_date_sa = new Date(run_start_date_full).toISOString();
					run_start_date = run_start_date_sa.split("T")[0];
					run_start_time = run_start_date_sa.split("T")[1];
					}
					
				}else{
					run_start_date_sa = new Date(run_start_date_full).toISOString();
					run_start_date = run_start_date_sa.split("T")[0];
					run_start_time = run_start_date_sa.split("T")[1];
				
				}if(run_end_date_full==undefined){
					run_end_date_full = $(this).closest('tr').find('td:eq(8)').text();
					
					if(run_end_date_full==""){
					run_end_date = "";
					run_end_time = "";
					}else{
						run_end_date_sa = new Date(run_end_date_full).toISOString();
						run_end_date = run_end_date_sa.split("T")[0];
						run_end_time = run_end_date_sa.split("T")[1];
					}
					
				}else{
					run_end_date_sa = new Date(run_end_date_full).toISOString();
					run_end_date = run_end_date_sa.split("T")[0];
					run_end_time = run_end_date_sa.split("T")[1];
				}
				
				var dataflowArray = {};
				dataflowArray['uuid'] = uuid;
				
				if (season != ""){
					dataflowArray['season'] = season;
				}if (creator != ""){
					dataflowArray['creator'] = creator;
				}if (flow_type != ""){
					dataflowArray['flow_type'] = flow_type;
				}if (run_start_date != ""){
					dataflowArray['run_start_date'] = run_start_date;
				}if (run_start_time != ""){
					dataflowArray['run_start_time'] = run_start_time;
				}if (run_end_date != ""){
					dataflowArray['run_end_date'] = run_end_date;
				}if (run_end_time != ""){
					dataflowArray['run_end_time'] = run_end_time;
				}
				
				
				
				$.ajax({
					type: "POST",
					//in this method insert the data in your database
					url: textit_prefix + "/flows",
					contentType: "application/json; charset=utf-8",
					data: JSON.stringify(dataflowArray),
					
					dataType: "json",
					success: AjaxUpdateDataSucceeded,
					error: AjaxUpdateDataFailed
				});
			});
			
			} }
        } );
    } );
 
    $('#flow').DataTable( {
        dom: "Bflrtip",
		lengthMenu: [ 10, 50, 100 ],
        ajax: {
			url:  textit_prefix + "/flows",
            dataSrc : ""
		},
        columns: [
			{
                data: null,
                defaultContent: '',
                className: 'select-checkbox',
                orderable: false
            },
			{ data: "uuid", "sClass":"uuid_hide"},
			{ data: "name" ,			 
			"render": function (data) {
        		var start_name = data.split("flow")[0];
				var end_name = data.split("flow")[1];
        		return (start_name + "flow" + "</br>" + end_name);
    			}
	 		},
			{ data: "runs", "visible": false},
			{ data: "completed_runs", "visible": false},
			{ data: "created_on",
				type: "datetime-moment", target:0,
				"render": function (data) {
				if (data != undefined){
        		var date = new Date(data).toISOString();
				var mom_date = moment(date);
				var new_date = mom_date.tz('Africa/Johannesburg').format('YYYY MMMM Do, h:mm a');
				var final_date = new_date.split(",");
        		return (final_date[0] + "," + final_date[1]);
					//return new_date;
				}
				
    			}
	 		},
			{ data: "country", "visible": false},
			{ data: "season" },
			{ data: "creator" },
			{ data: "flow_type" },
			{ data: "run_start_date",
			  type: "datetime-moment", target:0,
			 "render": function (data) {
				if (data != undefined){
				var mom_date = moment(data);
				var new_date = mom_date.tz('Africa/Johannesburg').format('YYYY MMMM Do');
        		return (new_date);
				}
				}
			},
			{ data: "run_end_date",
			  type: "datetime-moment", target:0,
			 "render": function (data) {
				if (data != undefined){
				var mom_date = moment(data);
				var new_date = mom_date.tz('Africa/Johannesburg').format('YYYY MMMM Do');
        		return (new_date);
				}
				}
			}
        ],
		
        select: {
            style:    'os',
            selector: 'td:first-child'
        },
        buttons: [
            { extend: "edit", editor: editor }
        ]
    } );

} );

function AjaxUpdateDataSucceeded(result) {
    if (result != "[]") {
		$('#success').html(""); //Clear error span
    	$('#success').append( "<span class='alert alert-success'><strong>Success! </strong>" + ' ' +" Metadata field successfully updated" + "<br></span>");
		setTimeout(function(){
		   window.location.reload();
		}, 10000);
		
    }
}

function AjaxUpdateDataFailed(result) {
	//alert(result.responseText);
    //alert(result.status + ' ' + result.statusText);
	$('#failure').html(""); //Clear error span
    $('#failure').append("<span class='alert alert-danger'><strong>Failure! </strong>" + ' ' + result.responseText + "<br></span>");
	setTimeout(function(){
		   window.location.reload();
	}, 10000);
}






