package de.uni_mannheim.informatik.dws.tnt.match.metanome;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.backend.input.file.DefaultFileInputGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * uses column index instead of name as header & identifier (important if headers are empty!)
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class T2TFileInputGenerator extends DefaultFileInputGenerator {

  protected T2TFileInputGenerator() {
  }

  public T2TFileInputGenerator(File inputFile) throws FileNotFoundException {
	  super(inputFile);
  }

  public T2TFileInputGenerator(ConfigurationSettingFileInput setting)
    throws AlgorithmConfigurationException {
	  super(setting);
  }

  @Override
  public RelationalInput generateNewCopy() throws InputGenerationException {
    try {
      return new T2TFileIterator(inputFile.getName(), new FileReader(inputFile), setting);
    } catch (FileNotFoundException e) {
      throw new InputGenerationException("File not found!", e);
    } catch (InputIterationException e) {
      throw new InputGenerationException("Could not iterate over the first line of the file input", e);
    }
  }

}
