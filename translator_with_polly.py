import streamlit as st
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import os
import sys
from tempfile import gettempdir
from contextlib import closing


aws_access_key="{}"
aws_secret_key="{}"

session=boto3.Session(aws_access_key_id =aws_access_key,aws_secret_access_key = aws_secret_key,region_name ='{}')

translate = session.client('translate')

polly = session.client("polly")



def translate_text(text, source_language,destination_language):
    """

    :param text: Input text which has to be translated
    :param source_language: The Input Language
    :param destination_language: The desired output Language
    :return:
    """
    result = translate.translate_text(
        Text=text,
        SourceLanguageCode=source_language,
        TargetLanguageCode=destination_language
    )
    return result['TranslatedText']


def text_to_speech(text_part):
    """
    :param text_part: The text which has to be converted to Hindi audio
    :return: temporary path where the audio is stored
    """

    print("Input text part for text to speech conversion : ",text_part)
    try:
        # Request speech synthesis
        response = polly.synthesize_speech(Text=text_part, LanguageCode="hi-IN",OutputFormat="mp3",
                                           VoiceId="Joanna")
    except (BotoCoreError, ClientError) as error:
        # The service returned an error, exit gracefully
        print(error)
        sys.exit(-1)
    # Access the audio stream from the response
    if "AudioStream" in response:
        # Note: Closing the stream is important because the service throttles on the
        # number of parallel connections. Here we are using contextlib.closing to
        # ensure the close method of the stream object will be called automatically
        # at the end of the with statement's scope.
        with closing(response["AudioStream"]) as stream:
            output = os.path.join(gettempdir(), "speech.mp3")

            try:
                # Open a file for writing the output as a binary stream
                with open(output, "wb") as file:
                    file.write(stream.read())
            except IOError as error:
                # Could not write to file, exit gracefully
                print(error)
                sys.exit(-1)
            print("Output Path where audio is stored :",output)
            return output





def runner():
    col11, col22 = st.columns(2)
    with col11:
        st.title("Language Translation")
    st.markdown('Feel the power of Neural Machine Translation')
    with col22:
        st.image('Capture.PNG', use_column_width=True);
    col1, col2 = st.columns(2)
    conversion_list={"English":"en","Bengali":"bn","Hindi":"hi","French":"fr"}
    with col1:
        source_language = st.selectbox('Select the Source Language', ['Default', 'English', 'Bengali','Hindi','French'])
        input_text = st.text_input('Enter the Input text', 'Enter text here')
    with col2:
        destination_language = st.selectbox('Select the Destination', ['Default', 'English', 'Bengali','Hindi','French'])

    with col1:
        button_value = st.checkbox(label='Translate')
    translated_text=""
    if button_value:
        if source_language!=destination_language:
            print("The Source Language is {}".format(conversion_list[source_language]))
            print("The Destination Language is {}".format(conversion_list[destination_language]))
            translated_text=translate_text(input_text, conversion_list[source_language],conversion_list[destination_language])
        else:
            translated_text=input_text
    with col2:
        st.text_input('Translated Text', translated_text)
        print("Translated Text : ",translated_text)
        if (destination_language == 'Hindi'):
            button_value_text_to_speech = st.checkbox(label='Audio Form')
            if(button_value_text_to_speech):
                audio_path = text_to_speech(translated_text)
                audio_file = open(audio_path, 'rb')
                audio_bytes = audio_file.read()
                st.audio(audio_bytes, format='audio / ogg')



runner()




