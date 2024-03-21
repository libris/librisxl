package whelk.housekeeping;

import whelk.Whelk;
import whelk.component.PostgreSQLComponent;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static whelk.util.Jackson.mapper;

public class ImageLinker extends HouseKeeper {
    private final String IMAGES_STATE_KEY = "linkedNewImagesUpTo";
    private String status = "OK";
    private final Whelk whelk;

    public ImageLinker(Whelk whelk) {
        this.whelk = whelk;
    }

    public String getName() {
        return "Image linker";
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return "* * * * *";
    }

    public void trigger() {
        handleNewImages();
        // also handle new instances!
    }

    public void handleNewImages() {

        Timestamp linkNewImagesSince = Timestamp.from(Instant.now().minus(2, ChronoUnit.DAYS));
        Map linkerState = whelk.getStorage().getState(getName());
        if (linkerState != null && linkerState.containsKey(IMAGES_STATE_KEY))
            linkNewImagesSince = Timestamp.from( ZonedDateTime.parse( (String) linkerState.get(IMAGES_STATE_KEY), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() );
        Instant linkedNewImagesUpTo = linkNewImagesSince.toInstant();

        String newImagesSql = """
                SELECT
                  data#>'{@graph,1,@id}' as imageUri, data#>>'{@graph,1,indirectlyIdentifiedBy}' as indirectlyIdentifiedBy, data#>>'{@graph,1,imageOf}' as imageOf, created
                FROM
                  lddb
                WHERE
                  collection = 'none' AND
                  data#>'{@graph,0,inDataset}' @> '[{"@id": "https://id.kb.se/dataset/images"}]'::jsonb AND
                  data#>>'{@graph,1,@type}' = 'ImageObject' AND
                  deleted = false AND
                  created > ?;
                """.stripIndent();

        try (Connection connection = whelk.getStorage().getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(newImagesSql)) {

            statement.setTimestamp(1, linkNewImagesSince);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Instant created = resultSet.getTimestamp("created").toInstant();
                    String imageUri = resultSet.getString("imageUri");
                    String imageOfUriString = resultSet.getString("imageOf");
                    String indirectIDsString = resultSet.getString("indirectlyIdentifiedBy");

                    List imageOfUriObjects = mapper.readValue(imageOfUriString, List.class);
                    List indirectIDsObjects = mapper.readValue(indirectIDsString, List.class);

                    if (created.isAfter(linkedNewImagesUpTo))
                        linkedNewImagesUpTo = created;

                    System.err.println("New image: " + imageUri + " on: " + indirectIDsObjects + " / on-direct : " + imageOfUriObjects);

                    List<String> recordsThatShouldLinkToImage = new ArrayList<>();
                    for (Object imageOf : imageOfUriObjects) {
                        if (imageOf instanceof Map imageOfMap) {
                            recordsThatShouldLinkToImage.add( (String) imageOfMap.get("@id") );
                        }
                    }
                    for (Object indirectID : indirectIDsObjects) {
                        if (indirectID instanceof Map indirectIDmap) {
                            if (indirectIDmap.get("@type").equals("ISBN")) {
                                List<String> uris = getUrisByISBN((String) indirectIDmap.get("value"));
                                recordsThatShouldLinkToImage.addAll(uris);
                            }
                        }
                    }

                    System.err.println("Should link the following to " + imageUri + " : " + recordsThatShouldLinkToImage);
                }
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString();
            throw new RuntimeException(e);
        }

        /*
        if (linkedNewImagesUpTo.isAfter(linkNewImagesSince.toInstant())) {
            Map<String, String> newState = new HashMap<>()
            newState.put(IMAGES_STATE_KEY, linkedNewImagesUpTo.atOffset(ZoneOffset.UTC).toString());
            whelk.getStorage().putState(getName(), newState);
        }*/
    }

    private List<String> getUrisByISBN(String isbn) {
        List<String> result = new ArrayList<>();

        String getByISBNsql = """
                SELECT
                  data#>>'{@graph,1,@id}' as uri
                FROM
                  lddb
                WHERE
                  deleted = false AND
                  collection = 'bib' AND
                  data#>'{@graph,1,identifiedBy}' @> ?
                """.stripIndent();

        try(PostgreSQLComponent.ConnectionContext ignored = new PostgreSQLComponent.ConnectionContext(whelk.getStorage().connectionContextTL)) {
            try (PreparedStatement statement = whelk.getStorage().getMyConnection().prepareStatement(getByISBNsql)) {
                statement.setObject(1, "[{\"@type\": \"ISBN\", \"value\": \"" + isbn + "\"}]", java.sql.Types.OTHER);
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    result.add( resultSet.getString("uri") );
                }
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString();
            throw new RuntimeException(e);
        }

        return result;
    }

    private void linkImage(String linkingRecordUri, String imageUri) {

    }
}
