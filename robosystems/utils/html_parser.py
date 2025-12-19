import os

from bs4 import BeautifulSoup


def extract_structured_content(html_content):
  """
  Process HTML content to extract structured text while preserving formatting

  Args:
      html_content: Raw HTML content as string

  Returns:
      structured_text: A formatted text representation of the HTML
  """
  # Parse HTML content
  soup = BeautifulSoup(html_content, "html.parser")

  # Extract main content
  main_content = soup.select_one("body")
  if not main_content:
    raise ValueError("Could not extract content from HTML document")

  # Process HTML to preserve structure
  structured_text = []

  # Process headings
  for heading in main_content.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
    # Add a clean version of the heading with level indicators
    if heading.name and len(heading.name) > 1:
      level = int(heading.name[1])
      heading_text = heading.get_text(strip=True)
      if heading_text:
        structured_text.append("\n" + "#" * level + " " + heading_text + "\n")

  # Process tables - important for financial data
  for table in main_content.find_all("table"):
    structured_text.append("\n--- TABLE START ---\n")

    # Process table headers
    headers = []
    for th in table.find_all("th"):
      headers.append(th.get_text(strip=True))

    if headers:
      structured_text.append(" | ".join(headers))
      structured_text.append(
        "-" * (sum(len(h) for h in headers) + 3 * (len(headers) - 1))
      )

    # Process table rows
    for tr in table.find_all("tr"):
      row_data = []
      for td in tr.find_all(["td"]):
        cell_text = td.get_text(strip=True).replace("\n", " ")
        row_data.append(cell_text)
      if row_data:
        structured_text.append(" | ".join(row_data))

    structured_text.append("--- TABLE END ---\n")

  # Process paragraphs
  for para in main_content.find_all("p"):
    para_text = para.get_text(separator=" ", strip=True)
    if para_text:
      structured_text.append(para_text + "\n")

  # Process div elements that might contain content
  for div in main_content.find_all("div"):
    # Skip if it contains other structured elements we already processed
    if div.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "table"]):
      continue

    div_text = div.get_text(separator=" ", strip=True)
    if div_text and len(div_text) > 20:  # Only include substantial content
      structured_text.append(div_text + "\n")

  # Process spans that might contain text not in other elements
  for span in main_content.find_all("span"):
    if (
      span.parent
      and span.parent.name
      and span.parent.name not in ["p", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th"]
    ):
      span_text = span.get_text(strip=True)
      if span_text and len(span_text) > 20:  # Only include substantial content
        structured_text.append(span_text + "\n")

  # Combine all the structured text
  full_text = "\n".join(structured_text)

  # Remove excessive blank lines
  full_text = "\n".join(line for line in full_text.split("\n") if line.strip())

  return full_text


def save_structured_content(html_content, url, output_dir="./data/input"):
  """
  Extract structured content from HTML and save to a file

  Args:
      html_content: Raw HTML content as string
      url: Source URL (used to generate filename)
      output_dir: Directory to save the file

  Returns:
      tuple: (output_path, structured_text, plain_text)
  """
  # Parse HTML and extract structured content
  structured_text = extract_structured_content(html_content)

  # Extract filename from URL
  if url and "/" in url:
    url_parts = url.split("/")
    if url_parts:
      last_part = url_parts[-1]
      if "." in last_part:
        filename = last_part.split(".")[0] + ".txt"
      else:
        filename = last_part + ".txt"
    else:
      filename = "unknown.txt"
  else:
    filename = "unknown.txt"

  # Ensure directory exists
  os.makedirs(output_dir, exist_ok=True)

  # Save structured text content to file
  output_path = os.path.join(output_dir, filename)
  with open(output_path, "w", encoding="utf-8") as f:
    f.write(structured_text)

  # Get plain text for processing
  soup = BeautifulSoup(html_content, "html.parser")
  main_content = soup.select_one("body")
  plain_text = main_content.get_text(separator=" ", strip=True) if main_content else ""

  return output_path, structured_text, plain_text
