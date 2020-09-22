var data_consumers = new Array();


function quick_stats(data)
{
    var total_cpus = 0;
    var active_cpus = 0;
    $.each(data['workers'], function(index, worker) {
        total_cpus += worker['cpus'];
        active_cpus += worker['active'];
    });
    $('#active_cpus').text(active_cpus);
    $('#total_cpus').text(total_cpus);

}

var usage_data = [
    {
        'label' : 'Usage',
        'data' : []
    },
    {
        'label' : 'Capacity',
        'data' : []
    },
    {
        'label' : 'Waiting Tasks',
        'data' : []
    },

];
var counter = 0;

function update_graph(data)
{

    var total_cpus = 0;
    var active_cpus = 0;
    var waiting_tasks = data['waiting_tasks'];
    $.each(data['workers'], function(index, worker) {
        total_cpus += worker['cpus'];
        active_cpus += worker['active'];
    });

    if( counter > 10 )
    {
        $.each(usage_data, function( index, data) {
            data['data'] = data['data'].splice(1);
        });
    }

    usage_data[0].data.push( [counter, active_cpus] );
    usage_data[1].data.push( [counter, total_cpus] );
    usage_data[2].data.push( [counter, waiting_tasks] );
    counter += 1;

    var options = {
        lines: { show: true },
        points: { show: true },
        xaxis: { tickDecimals: 0, tickSize: 10 }
    };
    var placeholder = $("#graph_placeholder");
    
    $.plot(placeholder, usage_data, options);
}

function data_update(data, textStatus, xhr)
{
    $.each(data_consumers, function( index, consumer) {
        consumer(data)
    });
    setTimeout(request_update, 4000);
}

function request_update()
{
    data_consumers.push( quick_stats );
    data_consumers.push( update_graph );
    $.getJSON('/data', data_update);
}

$(document).ready( function()  {
    request_update();
})
