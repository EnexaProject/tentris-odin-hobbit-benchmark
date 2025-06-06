package org.hobbit;

import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactoryHttp;
import org.aksw.jena_sparql_api.core.utils.UpdateRequestUtils;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.update.UpdateRequest;
import org.hobbit.core.rabbit.RabbitMQUtils;

public class MySystemAdapter extends AbstractSystemAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySystemAdapter.class);
    public String tentrisContName = null;
    private int selectsReceived = 0;
    private int selectsProcessed = 0;

    private int insertsReceived = 0;
    private int insertsProcessed = 0;
    private org.aksw.jena_sparql_api.core.QueryExecutionFactory queryExecFactory;
    private org.aksw.jena_sparql_api.core.UpdateExecutionFactory updateExecFactory;

    private AtomicInteger totalReceived = new AtomicInteger(0);
    private AtomicInteger totalSent = new AtomicInteger(0);

    private AtomicInteger totalRecievedURIs = new AtomicInteger(0);

    private volatile boolean phase2 = true;

    List<String> graphUris = new ArrayList<String>();

    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        internalInit();
        LOGGER.info("Initialization is over.");

    }

    /**
     * Internal initialization function. It builds the tentris image and runs
     * the container.
     *
     */
    public void internalInit() throws Exception {
        Map<String, String> environmentVariables = new HashMap<>();
        String env_value_IT_IS_ENEXA = "false";
        if (System.getenv().containsKey("IT_IS_ENEXA"))
        {
            env_value_IT_IS_ENEXA = System.getenv("IT_IS_ENEXA");
            environmentVariables.put("IT_IS_ENEXA",env_value_IT_IS_ENEXA);
        }


        String env_value_ENEXA_META_DATA_ENDPOINT = "";
        if(System.getenv().containsKey("ENEXA_META_DATA_ENDPOINT"))
        {
            env_value_ENEXA_META_DATA_ENDPOINT = System.getenv("ENEXA_META_DATA_ENDPOINT");
            environmentVariables.put("ENEXA_META_DATA_ENDPOINT",env_value_ENEXA_META_DATA_ENDPOINT);
        }

        String env_value_ENEXA_META_DATA_GRAPH = "";
        if(System.getenv().containsKey("ENEXA_META_DATA_GRAPH")){
            env_value_ENEXA_META_DATA_GRAPH = System.getenv("ENEXA_META_DATA_GRAPH");
            environmentVariables.put("ENEXA_META_DATA_GRAPH",env_value_ENEXA_META_DATA_GRAPH);
        }

        String env_value_ENEXA_MODULE_INSTANCE_IRI="";
        if (System.getenv().containsKey("ENEXA_MODULE_INSTANCE_IRI"))
        {
            env_value_ENEXA_MODULE_INSTANCE_IRI = System.getenv("ENEXA_MODULE_INSTANCE_IRI");
            environmentVariables.put("ENEXA_MODULE_INSTANCE_IRI",env_value_ENEXA_MODULE_INSTANCE_IRI);
        }


        tentrisContName = this.createContainer("hub.cs.upb.de/enexa/images/enexa-tentris-eval:latest", Constants.CONTAINER_TYPE_SYSTEM, toEnvironmentArray(environmentVariables),null,"/enexa","app1");

        if(tentrisContName==null || tentrisContName.isEmpty() ){
            LOGGER.error("name is EMPTY");
        }

        QueryExecutionFactory qef = FluentQueryExecutionFactory.http("http://" + tentrisContName + ":9080/sparql")
                .config().withPagination(50000).end().create();

        ResultSet testResults = null;
        while (testResults == null) {
            LOGGER.info("Using " + "http://" + tentrisContName + ":9080/sparql" + " to run test select query");

            // Create a QueryExecution object from a query string ... // and
            // runit
            QueryExecution qe = qef.createQueryExecution("SELECT * { ?s a <http://ex.org/foo/bar> } LIMIT 1");
            try {
                TimeUnit.SECONDS.sleep(2);
                testResults = qe.execSelect();
            } catch (Exception e) {
            } finally {
                qe.close();
            }
        }
        qef.close();

        // create execution factory
        queryExecFactory = new QueryExecutionFactoryHttp("http://" + tentrisContName + ":9080/sparql");
        // queryExecFactory = new
        // QueryExecutionFactoryPaginated(queryExecFactory, 100);

        // create update factory
        //HttpAuthenticator auth = new SimpleAuthenticator("dba", "dba".toCharArray());
        updateExecFactory = new UpdateExecutionFactoryHttp("http://" + tentrisContName + ":9080/update");
    }

    @Override
    public void receiveGeneratedData(byte[] arg0) {
        LOGGER.info("receiveGeneratedData phase2 is {}, arg ", phase2);//, new String(arg0, java.nio.charset.StandardCharsets.UTF_8));
        if (phase2 == true) {
            ByteBuffer buffer = ByteBuffer.wrap(arg0);
            // read the graph URI
            String graphUri = RabbitMQUtils.readString(buffer);
            //LOGGER.info("Receiving graph URI "+ graphUri);
            graphUris.add(graphUri);
            this.totalReceived.incrementAndGet();
            LOGGER.info("total Recieve "+this.totalReceived.get());
        } else {
            LOGGER.info("receiveGeneratedData phase2 is {} and it should be false",phase2);
            LOGGER.info("INSERT SPARQL query received.");
            this.insertsReceived++;
            ByteBuffer buffer = ByteBuffer.wrap(arg0);
            // read the insert query
            String insertQuery = RabbitMQUtils.readString(buffer);
            LOGGER.info("INSERT SPARQL query has been processed.");
            LOGGER.info("Inserting " + insertQuery);
            // insert query
            UpdateRequest updateRequest = UpdateRequestUtils.parse(insertQuery);
            //LOGGER.info("updated " + updateRequest);

            try {
                updateExecFactory.createUpdateProcessor(updateRequest).execute();
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Couldn't execute " + updateRequest.toString());
                LOGGER.error("Couldn't execute " + insertQuery);
                throw new RuntimeException();
            }

            LOGGER.info("INSERT SPARQL query finished");
            this.insertsProcessed++;
        }
    }

    @Override
    public void receiveGeneratedTask(String arg0, byte[] arg1) {
        LOGGER.info("receiveGeneratedTask arg0: " + arg0 + " arg1: " + new String(arg1, java.nio.charset.StandardCharsets.UTF_8));
        this.selectsReceived++;
        String taskId = arg0;
        // read select query
        ByteBuffer buffer = ByteBuffer.wrap(arg1);
        String selectQuery = RabbitMQUtils.readString(buffer);

        // Create a QueryExecution object from a query string ...
        QueryExecution qe = queryExecFactory.createQueryExecution(selectQuery);
        // and run it.

        ResultSet results = qe.execSelect();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(outputStream, results);

        try {
            this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
        } catch (IOException e) {
            LOGGER.error("Got an exception while sending results to evaluation storage.", e);
            try {
                outputStream.close();
            } catch (IOException e1) {
                e1.printStackTrace();
                LOGGER.error("Couldn't close ByteArrayOutputStream");
                throw new RuntimeException();
            }
            throw new RuntimeException();
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Couldn't close ByteArrayOutputStream");
            throw new RuntimeException();
        }
        qe.close();

        //LOGGER.info("SELECT SPARQL query has been processed.");
        this.selectsProcessed++;
    }



    @Override
    public void receiveCommand(byte command, byte[] data) {
        LOGGER.info("receiveCommand {}", command);
        LOGGER.info("Raw data as string: " + new String(data, java.nio.charset.StandardCharsets.UTF_8));
        if (TentrisConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {
            LOGGER.info("BULK_LOAD_DATA_GEN_FINISHED");
            this.totalRecievedURIs.incrementAndGet();
            Thread thread = new Thread() {
                public void run() {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    int numberOfMessages = buffer.getInt();
                    LOGGER.info("numberOfMessages " + numberOfMessages);
                    boolean lastBulkLoad = buffer.get() != 0;
                    int messagesSent = totalSent.addAndGet(numberOfMessages);
                    LOGGER.info("numberOfSentMessages " + messagesSent);
                    int messagesReceived = totalReceived.get();
                    while (messagesSent != messagesReceived) {
                        LOGGER.info("Messages received and sent are not equal " + messagesSent + " " + messagesReceived);
                        try {
                            TimeUnit.SECONDS.sleep(2);
                        } catch (Exception e) { }
                        messagesReceived = totalReceived.get();
                    }
                    LOGGER.info("Bulk phase begins");
                    // Create graphs
                    LOGGER.info("graphUris size " + graphUris.size());
                    for (String uri : graphUris) {
                        String create = "CREATE GRAPH <" + uri + ">";
                        UpdateRequest updateRequest = UpdateRequestUtils.parse(create);
                        LOGGER.info("updated query is : " + updateRequest);
                        updateExecFactory.createUpdateProcessor(updateRequest).execute();
                    }
                    try {
                        LOGGER.info("send to que");
                        sendToCmdQueue(TentrisConstants.BULK_LOADING_DATA_FINISHED);
                    } catch (IOException e) {
                        e.printStackTrace();
                        LOGGER.error("Couldn't send signal to BenchmarkController that bulk phase is over");
                        throw new RuntimeException();
                    }
                    LOGGER.info("Bulk phase is over.");
                    LOGGER.info("changing tha phase to false");
                    phase2 = false;
                }
            };
            thread.start();
//            LOGGER.info("totalRecievedURIs: ",this.totalRecievedURIs.get());
//            if(this.totalRecievedURIs.get() == graphUris.size()){
//                LOGGER.info("changing tha phase to false");
//                phase2 = false;
//            }else{
//                LOGGER.info("phase does not changed ");
//            }

        }
        super.receiveCommand(command, data);
    }

    @Override
    public void close() throws IOException {
        if (this.insertsProcessed != this.insertsReceived) {
            LOGGER.error("INSERT queries received and processed are not equal:"+this.insertsProcessed+" "+this.insertsReceived);
        }
        if (this.selectsProcessed != this.selectsReceived) {
            LOGGER.error("SELECT queries received and processed are not equal:"+this.selectsProcessed+" "+this.selectsReceived);
        }

        try {
            LOGGER.info("Closing connection to Tentris");
            queryExecFactory.close();
        } catch (Exception e) {
            LOGGER.error("Couldn't close connection to Tentris", e);
        }

        try {
            LOGGER.info("Closing connection to Tentris for update queries");
            updateExecFactory.close();
        } catch (Exception e) {
            LOGGER.error("Couldn't close connection to Tentris - edit", e);
        }
        LOGGER.info("Stop container");
        this.stopContainer(tentrisContName);
        super.close();
        LOGGER.info("Tentris has stopped.");

    }

    private String[] toEnvironmentArray(Map<String, String> environmentVariables) {
        String[] toReturn =  environmentVariables.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .toArray(String[]::new);

        LOGGER.info("Environment variables: " + Arrays.toString(toReturn));
        return toReturn;
    }
}
