package org.apache.lucene.analysis;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * Removes words that are too long or too short from the stream.
 */
public final class CustomFilter extends TokenFilter {
  
  private TermAttribute termAtt;

  /**
   * Build a filter that removes words that are too long or too
   * short from the text.
   */
  public CustomFilter(TokenStream in)
  {
    super(in);
    termAtt = addAttribute(TermAttribute.class);
  }
  
  /**
   * Returns the next input Token whose term() is the right len
   */
  @Override
  public final boolean incrementToken() throws IOException {
	  
    // return the first non-stop word found
    while (input.incrementToken()) {
      char[] cs = termAtt.termBuffer();
      for(int i=0;i<cs.length;i++){
    	  if(Character.isLetter(cs[i]) || cs[i] == '-' || cs[i] == '_'){
    		  continue;
    	  }
    	  if(Character.isDigit(cs[i])){
    		  return false;
    	  }
    	  else {
    		  return false;
    	  }
      }
      
      return true;
      
    }
    // reached EOS -- return false
    return false;
  }
}
