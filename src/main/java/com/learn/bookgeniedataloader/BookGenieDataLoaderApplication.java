package com.learn.bookgeniedataloader;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.learn.bookgeniedataloader.author.Author;
import com.learn.bookgeniedataloader.author.AuthorRepository;
import com.learn.bookgeniedataloader.book.Book;
import com.learn.bookgeniedataloader.book.BookRepository;
import connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookGenieDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BookGenieDataLoaderApplication.class, args);
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	@PostConstruct
	public void start() {
		System.out.println("====================================== Application Started ======================================");

		System.out.println(authorDumpLocation);
		System.out.println(worksDumpLocation);

//		initAuthors();
		initWorks();

		System.out.println("====================================== Saved all records ========================================");
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject = new JSONObject(jsonString);

				// Construct Author Object
				Author author = new Author();
				author.setName(jsonObject.optString("name"));
				author.setPersonalName(jsonObject.optString("personal_name"));
				author.setId(jsonObject.optString("key").replace("/authors/", ""));

				// Persist using repository
				authorRepository.save(author);
			});

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void initWorks() {
		DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		Path path = Paths.get(worksDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject = new JSONObject(jsonString);

				// Construct Book Object
				Book book = new Book();
				book.setId(jsonObject.getString("key").replace("/works/", ""));
				book.setName(jsonObject.optString("title"));
				JSONObject descriptionObj = jsonObject.optJSONObject("description");
				if (descriptionObj != null) {
					book.setDescription(descriptionObj.optString("value"));
				}

				JSONObject publishedDateObj = jsonObject.optJSONObject("created");
				if (publishedDateObj != null) {
					String dateStr = publishedDateObj.getString("value");
					book.setPublishedDate(LocalDate.parse(dateStr, df));
				}

				JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
				if (coversJSONArr != null) {
					List<String> coverIds = new ArrayList<>();
					for (int i=0; i < coversJSONArr.length(); i++) {
						coverIds.add(String.valueOf(coversJSONArr.getInt(i)));
					}
					book.setCoverIds(coverIds);
				}

				JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
				if (authorsJSONArr != null) {
					List<String> authorIds = new ArrayList<>();
					for (int i=0; i < authorsJSONArr.length(); i++) {
						String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author")
											.getString("key").replace("/authors/", "");
						authorIds.add(authorId);
					}
					book.setAuthorIds(authorIds);

					List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
							.map(optionalAuthor -> {
								if (!optionalAuthor.isPresent()) return "Unknown Author";
								else return optionalAuthor.get().getName();
							}).collect(Collectors.toList());

					book.setAuthorNames(authorNames);
				}

				// Persist using repository
				bookRepository.save(book);
			});

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
